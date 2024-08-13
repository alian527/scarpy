import scrapy
import pandas as pd

class TieziSpider(scrapy.Spider):
    name = "tiezi_spider"
    allowed_domains = ["tieba.baidu.com"]

    def __init__(self, pages_to_crawl=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pages_to_crawl = int(pages_to_crawl)
        self.posts_data = []  # 用于存储帖子数据
        self.authors_data = []  # 用于存储每个发言者的主页URL及性别
        self.file_index = 1  # 用于文件命名的序号

    def start_requests(self):
        base_url = "https://tieba.baidu.com/f?ie=utf-8&kw=%E6%9C%89%E7%94%B7%E5%81%B7%E7%8E%A9&fr=search&pn="
        for i in range(self.pages_to_crawl):
            page_url = f"{base_url}{i * 50}"
            yield scrapy.Request(url=page_url, callback=self.parse)

    def parse(self, response):
        posts = response.xpath('//a[@class="j_th_tit "]')
        for post in posts:
            title = post.xpath('./@title').get().strip()
            link = post.xpath('./@href').get().strip()
            absolute_link = response.urljoin(link)

            # 存储帖子信息
            self.posts_data.append({
                'title': title,
                'link': absolute_link
            })

            # 进入帖子页面，解析发言者主页URL
            yield scrapy.Request(url=absolute_link, callback=self.parse_post)

            # 每100行保存一次帖子数据
            if len(self.posts_data) >= 100:
                self.save_posts_data()

    def parse_post(self, response):
        author_links = response.xpath('//a[contains(@class, "p_author_name")]/@href').getall()
        for link in author_links:
            absolute_link = response.urljoin(link.strip())

            yield scrapy.Request(
                url=absolute_link,
                callback=self.parse_author,
                meta={'author_link': absolute_link}
            )

    def parse_author(self, response):
        sex_class = response.xpath('//span[contains(@class, "userinfo_sex")]/@class').get()
        gender = "unknown"
        if sex_class:
            if "userinfo_sex_male" in sex_class:
                gender = "male"
            elif "userinfo_sex_female" in sex_class:
                gender = "female"

        self.authors_data.append({
            'author_link': response.meta['author_link'],
            'gender': gender
        })

        # 每100行保存一次发言者数据
        if len(self.authors_data) >= 100:
            self.save_authors_data()

    def save_posts_data(self):
        df_posts = pd.DataFrame(self.posts_data)
        df_posts.to_excel(f"tiezi_posts_data_{self.file_index}.xlsx", index=False)
        self.logger.info(f"帖子数据已保存到 tiezi_posts_data_{self.file_index}.xlsx 文件中")
        self.posts_data = []  # 清空数据列表
        self.file_index += 1

    def save_authors_data(self):
        df_authors = pd.DataFrame(self.authors_data)
        df_authors.to_excel(f"tiezi_authors_data_{self.file_index}.xlsx", index=False)
        self.logger.info(f"发言者主页URL及性别数据已保存到 tiezi_authors_data_{self.file_index}.xlsx 文件中")
        self.authors_data = []  # 清空数据列表
        self.file_index += 1

    def close(self, reason):
        # 保存剩余未达到100行的数据
        if self.posts_data:
            self.save_posts_data()

        if self.authors_data:
            self.save_authors_data()

        self.logger.info("爬取结束，所有数据已保存")