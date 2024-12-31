import sys
import os
import re
import traceback
from logging import exception
from typing import Dict, List, Set, Tuple, Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from Settings import (
    JA_CRAWL_STATUS_MASTER as CRAWL_STATUS,
    MAX_PAGE_SOURCE_LENGTH,
    MAX_ERROR_LOG_LENGTH,
    JOB_EXLUDE_KEYWORDS,
    HTML_TAG_SEPERATOR
)
from main.GenericWebCrawlerAdapter import GenericWebCrawlerBaseAdapterClass

class JobscoreClass(GenericWebCrawlerBaseAdapterClass):
    def __init__(self, DailySpiderID: int, PatternID: int, CallDescOnly: bool = False, **kwargs):
        super().__init__(kwargs.get('DBEnv'), DailySpiderID, PatternID)
        self.CDMSID = kwargs.get('CDMSID', 0)
        self.URLSNoList = kwargs.get('URLSNoList', [])
        self.PyResourceName = kwargs.get('PyResourceName', '').strip()
        self.CallDescOnly = CallDescOnly
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }

    def __repr__(self) -> str:
        return self.__class__.__name__

    @property
    def CurrentFilePath(self) -> str:
        return str(os.path.abspath(__file__))

    def getCompanyList(self, **kwargs) -> List:
        return self.getCompanies(self.CDMSID, self.URLSNoList, self.PyResourceName, **kwargs)

    def extract_job_metadata(self, subtitle_tags) -> Tuple[str, str]:
        """Extract job category and employment type from subtitle tags."""
        JobCategory = ''
        EmploymentType = ''
        
        if subtitle_tags:
            for subtitle in subtitle_tags:
                subtitle_text = subtitle.text.strip()
                match = re.split(r'\s*\|\s*', subtitle_text)
                if len(match) >= 3:
                    JobCategory = match[0].strip()
                    EmploymentType = match[2].strip()
                    break
                    
        return JobCategory, EmploymentType

    def process_job_listing(self, job_item: Dict, company_data: Dict, output_table: str) -> Tuple[int, int, Set[str]]:
        """Process individual job listing and return success/failure counts and errors."""
        success_count = 0
        failed_count = 0
        error_logs = set()

        try:
            job_data = {
                'JobTitle': job_item.find('a').text.strip(),
                'JobURL': f"https://careers.jobscore.com/{job_item.find('a')['href']}",
                'JobLocation': self._get_job_location(job_item),
                'JobPostDate': self._get_job_post_date(job_item),
                'JobDesc': self._get_job_description(job_data['JobURL']),
                'OutputTableName': output_table
            }

            # Extract category and employment type
            subtitle_tags = job_item.find_all('h2', class_='js-subtitle')
            job_data['JobCategory'], job_data['EmploymentType'] = self.extract_job_metadata(subtitle_tags)

            db_status, error_log = self.insertJob(company_data['Sno'], **job_data)
            if db_status:
                success_count += 1
            else:
                failed_count += 1
                error_logs.add(error_log)

        except Exception as e:
            failed_count += 1
            error_logs.add(str(traceback.format_exc()))
            self.logger.error("Job extraction error: %s", traceback.format_exc())

        return success_count, failed_count, error_logs

    def _get_job_location(self, job_item) -> str:
        """Extract job location from job item."""
        try:
            location = job_item.find('span', class_="js-job-location").text.strip()
            return str(self.getcleanText(location, Items=["%"])).replace("'", '"')
        except Exception:
            return 'Global'

    def _get_job_post_date(self, job_item) -> str:
        """Extract job posting date from job item."""
        try:
            return job_item.find("div", class_="posting-date").text
        except Exception:
            return self.CurrentDateStr

    def _get_job_description(self, job_url: str) -> str:
        """Extract job description from job URL."""
        try:
            _, desc_response = self.sendRequest("GET", job_url, headers=self.headers, verify=False)
            _, soup_result = self.getBSoupResult(desc_response.text, Name='lxml')
            desc_text = " ".join(soup_result.find("div", {'id': "js-job-description"}).text.split())
            return self.getcleanText(desc_text, Items=['\n', '\t', '\r', '\xa0', "'", '"'])
        except Exception:
            return ''

    def startCrawl(self, CompanyData: Dict, HTMLTagData: Dict, CrawlStatusRecord: Dict, **kwargs) -> Dict:
        pattern_func_name = kwargs['FunctionName']
        start_time = self.getTime()
        error_logs = set()
        success_total, failed_total = 0, 0

        self.logger.info("Starting crawl for %s", pattern_func_name)

        try:
            domain = CompanyData['jobUrl'].replace('/feed.atom', "").replace('/feed.json', "")
            
            if not self.CallDescOnly:
                success_total, failed_total, error_logs = self._process_job_listings(
                    domain, CompanyData, kwargs['OutputTableName']
                )

            if self.CallDescOnly:
                success_total = CrawlStatusRecord.get('SuccessTotal', 0)
                failed_total = CrawlStatusRecord.get('FailedTotal', 0)

            # Update job descriptions
            self._update_job_descriptions(CompanyData['Sno'], kwargs['OutputTableName'])

            error_log = self.formatErrorSet(error_logs)
            status = CRAWL_STATUS["Failed"] if failed_total > 0 else CRAWL_STATUS["Success"]

        except Exception as ex:
            error_logs.add(str(traceback.format_exc()))
            error_log = self.formatErrorSet(error_logs)
            status = CRAWL_STATUS['Failed']
            self.logger.error("Crawling error: %s", traceback.format_exc())

        crawl_status_data = {
            "CrawlStatus": status,
            "SuccessTotal": success_total,
            "FailedTotal": failed_total,
            "ErrorLog": error_log[:MAX_ERROR_LOG_LENGTH]
        }

        self.logger.info(
            "Process ended. Time elapsed: %s minutes", 
            self.getElapsedTitme(start_time)
        )
        
        return crawl_status_data

if __name__ == '__main__':
    crawler = JobscoreClass(
        DailySpiderID=3,
        PatternID=73,
        URLSNoList=[110987]
    )
    crawler.run()
