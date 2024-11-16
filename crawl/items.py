# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DataotoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    car_name = scrapy.Field()
    car_price = scrapy.Field()
    year = scrapy.Field()
    madein = scrapy.Field()
    district = scrapy.Field()
    style = scrapy.Field()
    kilometer = scrapy.Field()
    gearbox = scrapy.Field()
    province = scrapy.Field()
    fuel = scrapy.Field()
    descri = scrapy.Field()
    pass
