from ..models import URL, Domain
from requests import get
from re import compile, sub, findall
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
from celery import shared_task
from celery_progress.backend import ProgressRecorder
from gzip import GzipFile
from io import BytesIO
from .save_to_sheet import save_db_to_sheets


class UpdateStatus:
    def __init__(self, progress_recorder):
        self._progress_recorder = progress_recorder

    def set_update_progress(self,
                            current_job,
                            total_work_to_do,
                            desc):
        self._progress_recorder.set_progress(current_job, total_work_to_do,
                                             description=f"{desc}")

    def error_revoke(self,
                     desc):
        self._progress_recorder.set_progress(0, 1,
                                             description=f"Error {desc}")


@shared_task(bind=True)
def choice_cms(self,
               domain: str,
               token: str,
               cms: str):
    update_status = UpdateStatus(ProgressRecorder(self))
    saas = SaaS(domain=domain, token=token, update_status=update_status)
    update_status.set_update_progress(1, 2, desc="Rozpoczynam proces")
    # if cms is shoper
    if cms == "Shoper":
        # start process with shoper parameter
        saas.shoper()
    elif cms == "IaI":
        saas.iai()
    elif cms == "SkyShop":
        saas.skyshop()
    # if cms is custom, get parameters from db. The parameter was entered on the frontend
    elif cms == "Custom":
        custom_domain = Domain.objects.get(token=token)

        # all parameters are optional

        # text_two_class and text_one_class - parameter (html id or class) where are we going to get the texts
        if custom_domain.text_two_class:
            custom_class_text = "("+str(custom_domain.text_one_class)+")" + \
                                "|" + "("+str(custom_domain.text_two_class)+")"
        else:
            # if custom_domain.text_one_class wasn't entered this custom_class_text = ""
            custom_class_text = str(custom_domain.text_one_class)

        # in custom option we can chose sitemap option or import url list,
        # if we import urls list we don't need to crawl sitemap
        if len(URL.objects.filter(token=token)) == 0:

            # we can put parameters that will be retrieved from the sitemap
            if custom_domain.condition_allow:
                condition_allow = custom_domain.condition_allow
            else:
                condition_allow = None
            # we can put parameters that will not be retrieved from the sitemap
            if custom_domain.condition_disallow:
                condition_disallow = custom_domain.condition_disallow
            else:
                condition_disallow = None

            GetUrlFromSitemap(token=token,
                              update_status=update_status).get_url_from_sitemaps_to_db(
                                                           sitemap_url=[sitemap_url
                                                                        for sitemap_url
                                                                        in str(custom_domain.sitemap_url).split("; ")],
                                                           condition_allow=condition_allow,
                                                           condition_disallow=condition_disallow)

        regex_conteiner = {'arrt_regex_text':
                               {str(custom_domain.class_id_text): compile(custom_class_text)},
                           'arrt_regex_product_area':
                               {str(custom_domain.class_id_product): compile(custom_domain.product_area)},
                           'arrt_regex_product_name':
                               {str(custom_domain.class_id_product): compile(custom_domain.product_id)},
                           'html_code_symbol':
                               compile("\w+"),
                           'sleep_time':
                               int(custom_domain.sleep_time)}

        saas.start_crawl(regex_conteiner=regex_conteiner)

    # after crawl we save data to google sheets
    if Domain.objects.get(token=token).status != "Dead":
        update_status.set_update_progress(current_job=1, total_work_to_do=2, desc="Save data to Google Sheets")
        save_db_to_sheets(token=token)
        update_status.set_update_progress(current_job=2, total_work_to_do=2, desc="Done!!")
        return "Done!!"
    else:
        raise update_status.error_revoke("Crawla dead")


class SaaS:
    def __init__(self,
                 domain: str,
                 token: str,
                 update_status):
        self.domain = domain
        self.token = token
        self.update_status = update_status

    def shoper(self) -> None:
        sitemap_url = [self.domain+"console/integration/execute/name/GoogleSitemap/list/categories/locale/pl_PL/page/1",
                       self.domain+"console/integration/execute/name/GoogleSitemap/list/categories/locale/pl_PL/page/2",
                       self.domain+"console/integration/execute/name/GoogleSitemap/list/producers/locale/pl_PL/page/1",
                       self.domain+"console/integration/execute/name/GoogleSitemap/list/producers/locale/pl_PL/page/2"]

        condition_allow = None
        condition_disallow = r'^(\/c\/.*\/\d+\/\d+$)|(((?!/c/).)*\d+$)'

        # get urls list with cms parameters
        GetUrlFromSitemap(token=self.token, update_status=self.update_status
                          ).get_url_from_sitemaps_to_db(sitemap_url=sitemap_url,
                                                        condition_allow=condition_allow,
                                                        condition_disallow=condition_disallow)

        regex_conteiner = {'arrt_regex_text': {'class': compile('(.*)categorydesc resetcss row(.*)')},
                           'arrt_regex_product_area': {'class': compile('(.*)products viewphot(.*)')},
                           'arrt_regex_product_name': {'class': compile('(.*)productname(.*)')},
                           'html_code_symbol': compile('(div)|p|(h2)|(h3)'),
                           'sleep_time': 1}
        self.start_crawl(regex_conteiner)

    def iai(self) -> None:
        iai_map = get(f'{self.domain}sitemap.xml.gz').text
        all_url_from_sitemap = BeautifulSoup(iai_map).find_all("loc")
        sitemap_url = []
        for i in range(1):
            sitemap_url.append(all_url_from_sitemap[i].get_text())

        condition_allow = None
        condition_disallow = r'(-cinfo-)|(-chelp-)|(-cabout-)|(\.php)|' \
                             r'(products)|(blog)|(cms)|(pol_n)|' \
                             r'(about)|(terms)|(ser-pol)|(parameters)|(series)|' \
                             r'(product-pol)|(contact)|(specials)|(promotions)|' \
                             r'(newproducts)|(-pol.html)'

        GetUrlFromSitemap(token=self.token, update_status=self.update_status
                          ).get_url_from_sitemaps_to_db(sitemap_url=sitemap_url,
                                                        condition_allow=condition_allow,
                                                        condition_disallow=condition_disallow)
        regex_conteiner = {'arrt_regex_text': {'class': compile('(.*)description( |_)(.*)(cm|sub)$')},
                           'arrt_regex_product_area': {'class': compile('row clearfix'), 'id': compile('layout')},
                           'arrt_regex_product_name': {'class': compile('product.{0,3}name')},
                           'html_code_symbol': compile('(div)|a|(section)|(h2)|p'),
                           'sleep_time': 1}
        self.start_crawl(regex_conteiner)

    def skyshop(self) -> None:
        sitemap_url = [self.domain + "sitemap.xml"]

        condition_allow = None
        condition_disallow = r'(blog)|(news)|(\/n\/)|((,|-)p,?\d+)'

        GetUrlFromSitemap(token=self.token, update_status=self.update_status
                          ).get_url_from_sitemaps_to_db(sitemap_url=sitemap_url,
                                                        condition_allow=condition_allow,
                                                        condition_disallow=condition_disallow)

        regex_conteiner = {'arrt_regex_text': {'class': compile('(.*)category-description(.*)')},
                           'arrt_regex_product_area': {'class': 'col-sm-9'},
                           'arrt_regex_product_name': {'class': compile('(.*)product.name(.*)')},
                           'html_code_symbol': compile('(div)|a|(p)|(h2)'),
                           'sleep_time': 2}
        self.start_crawl(regex_conteiner)

    def start_crawl(self, regex_conteiner: dict) -> None:
        if len(URL.objects.filter(token=self.token)) != 0:
            Crawl(self.token, self.update_status).crawl(
                          arrt_regex_text=regex_conteiner['arrt_regex_text'],
                          arrt_regex_product_area=regex_conteiner['arrt_regex_product_area'],
                          arrt_regex_product_name=regex_conteiner['arrt_regex_product_name'],
                          html_code_symbol=regex_conteiner['html_code_symbol'],
                          sleep_time=regex_conteiner['sleep_time'])
        else:
            raise self.update_status.error_revoke("Sitemap error") 


class GetUrlFromSitemap:
    def __init__(self,
                 token: str,
                 update_status):
        self.token = token
        self.update_status = update_status
        self.url_list = set()
        self.total_work_to_do = 0
        self.current_job = 0

    def check_status(self) -> None:
        if Domain.objects.get(token=self.token).status != "proggres":
            raise TimeoutError

    def request_sitemap(self,
                        sitemap) -> list:
        # some sitemaps was zipped so we need unzip
        if ".xml.gz" in sitemap:
            r = get(sitemap, stream=True)
            html_sitemap = GzipFile(fileobj=BytesIO(r.content)).read()
        else:
            html_sitemap = get(sitemap).text
        html_sitemap = BeautifulSoup(html_sitemap, 'html.parser')

        # usually urls are placed in loc parameter
        all_url_from_sitemap = html_sitemap.find_all("loc")

        # if urls in sitemap not in loc
        if not all_url_from_sitemap:
            all_url_from_sitemap = html_sitemap.find_all("a")

        # return all urls in sitemap
        return all_url_from_sitemap

    def get_url_from_sitemap(self,
                             all_url_from_sitemap,
                             condition_allow,
                             condition_disallow):
        self.total_work_to_do += len(all_url_from_sitemap)

        # iteration all urls from sitemap
        for url in all_url_from_sitemap:
            url_text = url.get_text()

            # check is the ticket alive
            self.check_status()

            self.update_status.set_update_progress(self.current_job, self.total_work_to_do, desc="Crawl sitemapy")
            self.current_job += 1

            # statement with conditions
            if condition_allow:
                if findall(condition_allow, url_text):
                    pass
                else:
                    continue
            if condition_disallow:
                if findall(condition_disallow, url_text):
                    continue

            # create final urls list
            self.url_list.add(url_text)

    def save_to_db(self):
        if len(self.url_list) != 0:
            self.current_job = 0
            self.total_work_to_do = len(self.url_list)
            for url in self.url_list:
                self.check_status()
                self.update_status.set_update_progress(self.current_job,
                                                       self.total_work_to_do,
                                                       desc="Zapis adresów z sitemapy")
                # save final urls list
                URL(domain=Domain.objects.get(token=self.token),
                    crawled_url=url,
                    token=self.token).save()
                self.current_job += 1
        else:
            error_message = f"Sitemaps error"
            Domain.objects.filter(token=self.token).update(error_message=error_message)
            self.update_status.error_revoke("Sitemaps error")

    # main function in this class
    def get_url_from_sitemaps_to_db(self,
                                    sitemap_url,
                                    condition_allow,
                                    condition_disallow):

        try:
            for sitemap in sitemap_url:
                all_url_from_sitemap = self.request_sitemap(sitemap)
                self.get_url_from_sitemap(all_url_from_sitemap=all_url_from_sitemap,
                                          condition_allow=condition_allow,
                                          condition_disallow=condition_disallow)
            self.save_to_db()

        except TimeoutError:
            error_message = f"Cancele task"
            Domain.objects.filter(token=self.token).update(error_message=error_message)
            self.update_status.error_revoke("Cancele task")


class Crawl:
    def __init__(self,
                 token: str,
                 update_status):
        self.token = token
        self.update_status = update_status
        self.current_job = 0
        self.total_work_to_do = 0
        self.text = ""
        self.error_message = ''
        self.length = 0
        self.h1 = ""
        self.product_name = ""
        self.amount_of_products = 0
        self.headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0"}

    def get_response(self,
                     url: str) -> None:
        try:
            self.html = get(url, timeout=10, headers=self.headers)
            html_text = self.html.text
            self.soup = BeautifulSoup(html_text, features="lxml")
            
        except TimeoutError:
            self.error_message = "Soup error"
            self.error()
            
        except Exception as e:
            self.error_message = f"Error {e}"
            self.error()

    def get_text_from_resposne_url(self,
                                   arrt_regex_text: dict,
                                   html_code_symbol) -> None:
        try:
            # get text from container
            text_conteinter = self.soup.find_all(html_code_symbol, attrs=arrt_regex_text)
            self.text = ""
            self.length = 0
            for txt in text_conteinter:
                self.text += txt.get_text()
                self.length = self.length + len(txt.get_text())
        except:
            self.text = ""
            self.length = 0

    def get_h1_from_response_url(self) -> None:
        try:
            # get first H1 from page and clean extra space and next line
            self.h1 = self.soup.find("h1").get_text()
            self.h1 = sub(r"\s{2,}","", self.h1)
            self.h1 = sub(r"\n", "", self.h1)
            self.h1 = sub(r"^ | $", "", self.h1)
        except:
            self.h1 = ""

    def get_product_from_resposne_url(self,
                                      html_code_symbol,
                                      arrt_regex_product_area,
                                      arrt_regex_product_name) -> None:
        try:
            product_area_all = self.soup.find_all(html_code_symbol,
                                                  attrs=arrt_regex_product_area)

            # sometime if we enter id/class in custom option there are several area of the same id/class on page
            # so we need to find only this area where we get products
            product_name_list = list(filter(lambda product_area:
                                            product_area.find_all(html_code_symbol,
                                                                  attrs=arrt_regex_product_name), product_area_all))
            self.product_name = ""
            self.amount_of_products = 0
            for product in product_name_list:
                self.product_name += product.get_text() + "; "
                self.amount_of_products += 1
        except:
            self.product_name = ""
            self.amount_of_products = 0

    def error(self) -> None:
        self.text = ""
        self.product_name = ""
        self.amount_of_products = 0
        self.h1 = ""

    def save_in_db(self, url) -> None:
        URL.objects.filter(token=self.token, crawled_url=url).update(
            status_code=301 if self.html.history else self.html.status_code,
            text=self.text,
            h1=self.h1,
            crawl_date=datetime.now(),
            request_header=self.html.headers,
            text_length=self.length,
            amount_of_products=self.amount_of_products,
            product_name=self.product_name,
            error_message=self.error_message)

    # main class function
    def crawl(self,
              arrt_regex_text: dict,
              arrt_regex_product_area: dict,
              arrt_regex_product_name: dict,
              html_code_symbol,
              sleep_time: int) -> None:
        url_list = list(URL.objects.filter(token=self.token))
        self.total_work_to_do = len(url_list)

        # iteration all urls from sitemap/input
        for url in url_list:
            # need combine this check status with check_status from GetUrlFromSitemap
            if Domain.objects.get(token=self.token).status != "proggres":
                raise TimeoutError
            self.update_status.set_update_progress(self.current_job, self.total_work_to_do, desc="Crawl strony")
            self.error_message = ''
            # getResponse
            try:
                self.get_response(url)
                if self.html.status_code == 200:
                    self.get_product_from_resposne_url(arrt_regex_product_area=arrt_regex_product_area,
                                                       arrt_regex_product_name=arrt_regex_product_name,
                                                       html_code_symbol=html_code_symbol)
                    self.get_text_from_resposne_url(arrt_regex_text=arrt_regex_text,
                                                    html_code_symbol=html_code_symbol)
                    self.get_h1_from_response_url()
                else:
                    self.error_message = f"Page error - status code {self.html.status_code}"
                    self.error()
            except TimeoutError:
                break
            except Exception as e:
                self.error_message += f" - {e}"
                

            # save all data from single url
            self.save_in_db(url)
            self.current_job += 1

            # wait not to get banned
            sleep(sleep_time)
