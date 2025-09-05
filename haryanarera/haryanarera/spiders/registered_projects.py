import re
import scrapy

class RegisteredProjectsSpider(scrapy.Spider):
    name = "registered_projects"
    allowed_domains = ["haryanarera.gov.in"]
    start_urls = ["https://haryanarera.gov.in/admincontrol/registered_projects/1"]

    # def parse(self, response):
    #     # Select table rows except header
    #     rows = response.css("table tr")[1:]  

    #     for row in rows:
    #         yield {
    #             "reg_cert_no": row.css("td:nth-child(2)::text").get(),
    #             "project_id": row.css("td:nth-child(3)::text").get(),
    #             "project_name": row.css("td:nth-child(3)::text").get(),
    #             "builder": row.css("td:nth-child(4)::text").get(),
    #             "project_location": row.css("td:nth-child(5)::text").get(),
    #             "project_district": row.css("td:nth-child(6)::text").get(),
    #             "registered_with": row.css("td:nth-child(7)::text").get(),
    #                 "details_of_project_form_A-H": row.css("td:nth-child(8)::text").get(),
    #                     "registration_upto": row.css("td:nth-child(9)::text").get(),
    #                         "certificate": row.css("td:nth-child(10)::text").get(),
    #                                 "view_quarterly_progress": row.css("td:nth-child(11)::text").get(),
    #                                     "monitoring_orders": row.css("td:nth-child(12)::text").get(),
    #                                         "view_OC_CC_PCC": row.css("td:nth-child(13)::text").get(),
                                                

    #         }

    #     # Handle pagination if there are more pages
    #     next_page = response.css("a[rel='next']::attr(href)").get()
    #     if next_page:
    #         yield response.follow(next_page, callback=self.parse)


    def parse(self, response):
        table = response.css("table#compliant_hearing")
        rows = table.css("tr")[1:]  # skip header row

        for row in rows:
            cols_data = []

            for td in row.css("td"):
                anchor = td.css("a")
                if anchor:
                    text = anchor.css("::text").get(default="").strip()
                    
                    # 1️⃣ Try normal href first
                    link = anchor.attrib.get("href", "").strip()

                    # 2️⃣ If no href, check onclick
                    if not link and "onclick" in anchor.attrib:
                        onclick_val = anchor.attrib.get("onclick", "")
                        match = re.search(r"'(.*?)'", onclick_val)  # extract the first quoted string
                        if match:
                            link = match.group(1).strip()

                    # 3️⃣ Convert relative path to absolute URL
                    if link:
                        link = response.urljoin(link)

                    cols_data.append({"text": text, "link": link or None})
                else:
                    # No <a> tag, just plain text
                    text = td.css("::text").get(default="").strip()
                    cols_data.append({"text": text, "link": None})

            yield {
                f"col{i+1}": cols_data[i] if i < len(cols_data) else None
                for i in range(len(cols_data))
            }