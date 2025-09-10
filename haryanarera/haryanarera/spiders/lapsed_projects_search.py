import scrapy
import os
import random
import time
import requests
from bs4 import BeautifulSoup

class LapsedProjectsSpider(scrapy.Spider):
    name = "lapsed_projects_with_ddg"
    allowed_domains = ["haryanarera.gov.in", "duckduckgo.com"]
    start_urls = ["https://haryanarera.gov.in/admincontrol/lapsed_projects/1"]

    HEADERS = [
        "Serial No.",
        "Registration Certificate Number",
        "Project ID",
        "Project Name",
        "Builder",
        "Project Location",
        "Project District",
        "Approval From",
        "Approval To",
        "View Certificate"
    ]

    # DuckDuckGo HTML endpoint
    DDG_URL = "https://duckduckgo.com/html/"

    # User-Agent list for rotation
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0 Safari/537.36",
    ]

    PROXY_API = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=3000&country=all"
    proxies_list = []

    def start_requests(self):
        # Fetch free proxies before starting
        try:
            resp = requests.get(self.PROXY_API, timeout=10)
            self.proxies_list = [p.strip() for p in resp.text.strip().split("\n") if p.strip()]
            self.logger.info(f"Fetched {len(self.proxies_list)} proxies from ProxyScrape.")
        except Exception as e:
            self.logger.error(f"Error fetching proxies: {e}")
            self.proxies_list = []

        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        rows = response.css("#compliant_hearing tbody tr")
        row_limit = 50  # ✅ Limit to first 50 rows

        for index, row in enumerate(rows, start=1):
            if index > row_limit:
                break

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
                col_data.append({"text": text, "anchors": anchors if anchors else None})

            mapped_result = {}
            for i, header in enumerate(self.HEADERS):
                mapped_result[header] = col_data[i] if i < len(col_data) else {"text": "", "anchors": None}

            mapped_result["error"] = None

            # 🔍 Perform DuckDuckGo search
            search_query = f"{mapped_result['Project Name']['text']} {mapped_result['Builder']['text']} {mapped_result['Project Location']['text']} {mapped_result['Registration Certificate Number']['text']}"
            ddg_results = self.search_duckduckgo(search_query)
            mapped_result["duckduckgo_results"] = ddg_results

            yield mapped_result

    def search_duckduckgo(self, query):
       
        if not self.proxies_list:
            print('_______________________________')
            print(self.proxies_list)
            return []

        proxy = random.choice(self.proxies_list)
        headers = {"User-Agent": random.choice(self.USER_AGENTS)}
        params = {"q": query}

        try:
            resp = requests.get(
                self.DDG_URL,
                params=params,
                headers=headers,
                # proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
                timeout=10
            )
            soup = BeautifulSoup(resp.text, "html.parser")
            results = []

            for result in soup.select(".result"):
                title_tag = result.select_one(".result__a")
                snippet_tag = result.select_one(".result__snippet")
                link = title_tag["href"] if title_tag else None
                title = title_tag.get_text(strip=True) if title_tag else None
                snippet = snippet_tag.get_text(strip=True) if snippet_tag else None

                if title and link:
                    results.append({
                        "title": title,
                        "link": link,
                        "snippet": snippet
                    })

            # Sleep between searches to avoid bans
            time.sleep(random.uniform(3, 8))
            return results

        except Exception as e:
            self.logger.warning(f"DuckDuckGo search failed for '{query}' via {proxy}: {e}")
            return []
