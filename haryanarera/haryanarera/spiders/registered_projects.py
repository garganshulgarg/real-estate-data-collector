import scrapy
import re
import json
import os
import base64

class RegisteredProjectsSpider(scrapy.Spider):
    name = "registered_projects"
    allowed_domains = ["haryanarera.gov.in"]
    start_urls = ["https://haryanarera.gov.in/admincontrol/registered_projects/1"]

    def parse(self, response):
        rows = response.css("#compliant_hearing tbody tr")

        for row in rows:
            col_data = []

            for td in row.css("td"):
                text = " ".join(td.css("*::text").getall()).strip()

                anchors = []
                for a in td.css("a"):
                    anchors.append({
                        "text": a.css("::text").get(default="").strip(),
                        "href": response.urljoin(a.attrib.get("href", "").strip()) if a.attrib.get("href") else None,
                        "onclick": a.attrib.get("onclick", "").strip() if "onclick" in a.attrib else None
                    })

                col_data.append({
                    "text": text,
                    "anchors": anchors if anchors else None
                })

            # Base result without yielding here
            result = {
                "columns": col_data,
                "oc_cc_pcc": "",
                "error": None
            }

            # 📥 Step 1: Check if 11th column's 1st anchor has certificate PDF link
            if len(col_data) >= 11 and col_data[10]["anchors"]:
                first_anchor = col_data[10]["anchors"][0]
                href = first_anchor.get("href", "")
                if "https://haryanarera.gov.in/view_project/view_certificate" in href:
                    # folder: downloads/view_cert/{2nd column's text}
                    folder_name = col_data[1]["text"] or "unknown"
                    folder_path = os.path.join("downloads", "view_cert", folder_name)
                    os.makedirs(folder_path, exist_ok=True)

                    filename = os.path.basename(href)
                    if not filename.lower().endswith(".pdf"):
                        filename += ".pdf"
                    yield scrapy.Request(
                        url=href,
                        callback=self.save_file,
                        meta={"folder_path": folder_path, "filename": filename},
                        errback=self.handle_error
                    )

            try:
                last_col_text = col_data[-1]["text"].strip().lower()
                if "view oc/cc/pcc" in last_col_text:
                    onclick_value = None
                    for anchor in col_data[-1]["anchors"] or []:
                        if anchor.get("onclick") and "view_corrigendums" in anchor["onclick"]:
                            onclick_value = anchor["onclick"]
                            break
                    if onclick_value:
                        match = re.search(r"view_corrigendums\((\d+)\s*,", onclick_value)
                        if match:
                            project_id_raw = match.group(1)
                            project_id = base64.b64encode(project_id_raw.encode()).decode()  # btoa equivalent
                            form_data = {"project_id": project_id, "document_type": "OC"}

                            yield scrapy.FormRequest(
                                url="https://haryanarera.gov.in/assistancecontrol/view_corrigendums_popup",
                                formdata=form_data,
                                callback=self.parse_oc_cc_pcc,
                                meta={"base_result": result, "form_data": form_data},
                                errback=self.handle_error
                            )
                        else:
                            result["error"] = "Project ID not found in onclick"
                            yield result
                    else:
                        result["error"] = "No onclick found in last column anchors"
                        yield result
                else:
                    yield result  # No OC/CC/PCC case
            except Exception as e:
                self.logger.error(f"Parse error: {str(e)}")
                yield result

    def parse_oc_cc_pcc(self, response):
        base_result = response.meta["base_result"]
        form_data = response.meta["form_data"]
        base_result["oc_cc_pcc_status_code"] = response.status

        try:
            oc_data = json.loads(response.text)
            base_result["oc_cc_pcc"] = oc_data
            base_result["form_data"] = form_data

            folder_name = oc_data.get("registration_certificate_no", "unknown").strip()
            folder_path = os.path.join("downloads", folder_name)
            os.makedirs(folder_path, exist_ok=True)

            for doc in oc_data.get("corrigendum_list", []):
                file_url = doc.get("file_url")
                if file_url:
                    absolute_url = f"https://haryanarera.gov.in/{file_url.lstrip('/')}"
                    filename = os.path.basename(file_url)

                    yield scrapy.Request(
                        url=absolute_url,
                        callback=self.save_file,
                        meta={"folder_path": folder_path, "filename": filename},
                        errback=self.handle_error
                    )

        except Exception as e:
            base_result["error"] = f"OC/CC/PCC parse error: {str(e)}"

        yield base_result  # ✅ Yield only once, after processing

    def save_file(self, response):
        folder_path = response.meta["folder_path"]
        filename = response.meta["filename"]
        file_path = os.path.join(folder_path, filename)

        with open(file_path, "wb") as f:
            f.write(response.body)

        self.logger.info(f"File saved: {file_path}")

    def handle_error(self, failure):
        base_result = failure.request.meta.get("base_result", {})
        form_data = failure.request.meta.get("form_data", {})
        self.logger.error(f"Request failed for form_data {form_data}: {failure.value}")
        base_result["error"] = f"Request failed: {failure.value}"
        yield base_result
