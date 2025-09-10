import scrapy
import re
import json
import os
import base64

class RegisteredProjectsSpider(scrapy.Spider):
    name = "registered_projects_col_names"
    allowed_domains = ["haryanarera.gov.in"]
    start_urls = ["https://haryanarera.gov.in/admincontrol/registered_projects/1"]

    HEADERS = [
        "Serial No.",
        "Registration Certificate Number",
        "Project ID",
        "Project Name",
        "Builder",
        "Project Location",
        "Project District",
        "Registered With",
        "Details of Project(Form A-H)",
        "Registration Up-to",
        "View Certificate",
        "View Quarterly Progress",
        "Monitoring Orders",
        "View OC/CC/PCC"
    ]

    def parse(self, response):
        rows = response.css("#compliant_hearing tbody tr")

        for row in rows:
            row_data = {}
            columns = row.css("td")

            for idx, td in enumerate(columns):
                text = " ".join(td.css("*::text").getall()).strip()
                anchors = [
                    {
                        "text": a.css("::text").get(default="").strip(),
                        "href": response.urljoin(a.attrib.get("href", "").strip()) if a.attrib.get("href") else None,
                        "onclick": a.attrib.get("onclick", "").strip() if "onclick" in a.attrib else None
                    }
                    for a in td.css("a")
                ]
                row_data[self.HEADERS[idx]] = {
                    "text": text,
                    "anchors": anchors if anchors else None
                }

            # Base result
            result = {
                **row_data,
                "oc_cc_pcc": "",
                "error": None
            }

            # 📥 Step 1: Download View Certificate PDF (11th column)
            # view_cert_col = row_data.get("View Certificate")
            # if view_cert_col and view_cert_col["anchors"]:
            #     href = view_cert_col["anchors"][0].get("href", "")
            #     if "https://haryanarera.gov.in/view_project/view_certificate" in href:
            #         folder_name = row_data["Registration Certificate Number"]["text"] or "unknown"
            #         folder_path = os.path.join("downloads", "view_cert", folder_name)
            #         os.makedirs(folder_path, exist_ok=True)

            #         filename = os.path.basename(href)
            #         if not filename.lower().endswith(".pdf"):
            #             filename += ".pdf"

            #         yield scrapy.Request(
            #             url=href,
            #             callback=self.save_file,
            #             meta={"folder_path": folder_path, "filename": filename},
            #             errback=self.handle_error
            #         )

            # 📥 Step 2: Check View OC/CC/PCC column
            oc_col = row_data.get("View OC/CC/PCC")
            if oc_col and oc_col["text"].strip().lower().find("view oc/cc/pcc") != -1:
                onclick_value = None
                for anchor in oc_col["anchors"] or []:
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
                    result["error"] = "No onclick found in View OC/CC/PCC anchors"
                    yield result
            else:
                yield result

    def parse_oc_cc_pcc(self, response):
        base_result = response.meta["base_result"]
        form_data = response.meta["form_data"]
        base_result["oc_cc_pcc_status_code"] = response.status

        try:
            oc_data = json.loads(response.text)
            base_result["oc_cc_pcc"] = oc_data
            base_result["form_data"] = form_data

            # folder_name = oc_data.get("registration_certificate_no", "unknown").strip()
            # folder_path = os.path.join("downloads", folder_name)
            # os.makedirs(folder_path, exist_ok=True)

            # for doc in oc_data.get("corrigendum_list", []):
            #     file_url = doc.get("file_url")
            #     if file_url:
            #         absolute_url = f"https://haryanarera.gov.in/{file_url.lstrip('/')}"
            #         filename = os.path.basename(file_url)

            #         yield scrapy.Request(
            #             url=absolute_url,
            #             callback=self.save_file,
            #             meta={"folder_path": folder_path, "filename": filename},
            #             errback=self.handle_error
            #         )

        except Exception as e:
            base_result["error"] = f"OC/CC/PCC parse error: {str(e)}"

        yield base_result

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
