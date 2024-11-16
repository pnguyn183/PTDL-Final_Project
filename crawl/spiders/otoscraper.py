import scrapy
from crawl.items import DataotoItem
from scrapy.exceptions import DropItem

class OtoscraperSpider(scrapy.Spider):
    name = "otoscraper"
    allowed_domains = ["oto.com.vn"] 

    max_pages = 100  # Số trang tối đa bạn muốn thu thập

    def start_requests(self):
        # Bắt đầu từ trang đầu tiên
        yield scrapy.Request(url='https://oto.com.vn/mua-ban-xe/p2', callback=self.parse_list)

    def parse_list(self, response):
        # Lấy danh sách các liên kết xe máy từ trang
        product_links = response.xpath('//*[@id="box-list-car"]/div/div[1]/a/@href').getall()
        if product_links:
            for link in product_links:
                yield scrapy.Request(
                    url=response.urljoin(link),
                    callback=self.parse_product
                )   

        # Lấy số trang hiện tại từ URL
        current_page = int(response.url.split('p')[-1])

        # Kiểm tra nếu số trang hiện tại vượt quá giới hạn
        if current_page < self.max_pages:
            next_page = current_page + 1  # Chuyển sang trang tiếp theo
            next_page_url = f'https://oto.com.vn/mua-ban-xe/p{next_page}'

            # Yêu cầu trang tiếp theo nếu nó tồn tại
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_list  # Chuyển tiếp đến parse_list để lấy sản phẩm trên trang mới
            )
   

    def parse_product(self, response):
        # Khởi tạo item để lưu dữ liệu
        item = DataotoItem()
        item['car_name'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[1]/h1/text())').get()
        item['car_price'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[2]/div/span[1]/span/text())').get()
        item['year'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[1]/text())').get()
        item['style'] = response.xpath('//*[@id="box-detail"]/div[3]/div[2]/ul/li[2]/text()').get().strip()
        if item['style'] is None or item['style'].strip() == "":
            raise DropItem("Dropped item due to empty style")
        item['madein'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[4]/text())').get()
        item['kilometer'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[5]/text())').get()
        item['province'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[6]/div)').get()
        item['district'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[7]/div/text())').get()
        item['gearbox'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[8]/text())').get()
        item['fuel'] = response.xpath('normalize-space(//*[@id="box-detail"]/div[3]/div[2]/ul/li[9]/text())').get()
        item['descri'] = response.xpath('normalize-space(//*[@id="tab-desc"]/div[1]/div/text()[1])').get()    

        # Trả về item đã được cào
        yield item
