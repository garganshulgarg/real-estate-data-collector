import scrapy
import os

class LapsedProjectsSpider(scrapy.Spider):
    name = "lapsed_projects"
    allowed_domains = ["haryanarera.gov.in"]
    start_urls = ["https://haryanarera.gov.in/admincontrol/lapsed_projects/1"]

    # custom_settings = {
    #     "CONCURRENT_REQUESTS": 32,         # Default 16
    #     "CONCURRENT_REQUESTS_PER_DOMAIN": 16,  # Default 8
    #     "DOWNLOAD_DELAY": 0,               # No delay between requests
    #     "AUTOTHROTTLE_ENABLED": False,      # Disable auto-throttle
    #     "RETRY_ENABLED": True,
    #     "RETRY_TIMES": 2
    # }

    def parse(self, response):
        rows = response.css("#compliant_hearing tbody tr")
        row_limit = 50
        for index, row in enumerate(rows, start=1):
            if index > row_limit:
                break  # ✅ Stop after 50 rows
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

            result = {
                "columns": col_data,
                "error": None
            }

            # PDF download logic
            if len(col_data) >= 10 and col_data[9]["anchors"]:
                first_anchor = col_data[9]["anchors"][0]
                href = first_anchor.get("href", "")
                if "https://haryanarera.gov.in/view_project/view_certificate" in href:
                    folder_name = col_data[1]["text"] or "unknown"
                    folder_path = os.path.join("downloads", "lapsed_view_cert", folder_name)
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

            # ✅ Always yield the parsed table data
            yield result

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
