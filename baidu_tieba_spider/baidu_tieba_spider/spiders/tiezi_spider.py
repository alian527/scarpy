import scrapy
import pandas as pd

class TieziSpider(scrapy.Spider):
    name = "tiezi_spider"
    allowed_domains = ["tieba.baidu.com"]

    def __init__(self, pages_to_crawl=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pages_to_crawl = int(pages_to_crawl)
        self.authors_data = []  # 用于存储每条回复的所有信息
        self.file_index = 1  # 用于文件命名的序号

    def start_requests(self):
        base_url = "https://tieba.baidu.com/f?ie=utf-8&kw=%E6%9C%89%E7%94%B7%E5%81%B7%E7%8E%A9&fr=search&pn="
        for i in range(self.pages_to_crawl):
            page_url = f"{base_url}{i * 50}"
            yield scrapy.Request(url=page_url, callback=self.parse)

    def parse(self, response):
        posts = response.xpath('//a[@class="j_th_tit "]')
        for post in posts:
            link = post.xpath('./@href').get().strip()
            absolute_link = response.urljoin(link)

            # 进入帖子页面，解析每个回复的信息
            yield scrapy.Request(url=absolute_link, callback=self.parse_post)

    def parse_post(self, response):
        # 提取回帖信息
        author_names = response.xpath('//a[@class="frs-author-name j_user_card "]/text()').getall()
        author_links = response.xpath('//a[@class="frs-author-name j_user_card "]/@href').getall()
        reply_contents = response.xpath('//div[@class="d_post_content j_d_post_content "]//text()').getall()
        reply_times = response.xpath('//span[@class="tail-info" and contains(text(), "-")]/text()').getall()

        # 逐个处理每个回帖信息
        for name, link, content, time in zip(author_names, author_links, reply_contents, reply_times):
            absolute_link = response.urljoin(link.strip())
            author_data = {
                'author_name': name,
                'author_link': absolute_link,
                'reply_content': content.strip(),
                'reply_time': time
            }

            # 进入发帖者主页，提取性别、吧龄等信息后再返回并保存
            yield scrapy.Request(
                url=absolute_link,
                callback=self.parse_author,
                meta={'author_data': author_data}
            )

    def parse_author(self, response):
        author_data = response.meta['author_data']

        # 提取性别、吧龄、发帖数量和IP属地等信息
        sex_class = response.xpath('//span[contains(@class, "userinfo_sex")]/@class').get()
        gender = "unknown"
        if sex_class:
            if "userinfo_sex_male" in sex_class:
                gender = "male"
            elif "userinfo_sex_female" in sex_class:
                gender = "female"

        years_on_tieba = response.xpath('//span[contains(text(), "吧龄")]/text()').re_first(r'吧龄:(\d+(\.\d+)?)年')
        num_posts = response.xpath('//span[contains(text(), "发贴")]/text()').re_first(r'发贴:(\d+)')
        ip_location = response.xpath('//span[contains(text(), "IP属地")]/text()').re_first(r'IP属地:(.*)')

        # 更新作者数据字典
        author_data.update({
            'gender': gender,
            'years_on_tieba': years_on_tieba,
            'num_posts': num_posts,
            'ip_location': ip_location
        })

        # 将完整的数据加入列表
        self.authors_data.append(author_data)

        # 每100行保存一次发言者数据
        if len(self.authors_data) >= 100:
            self.save_authors_data()

    def save_authors_data(self):
        df_authors = pd.DataFrame(self.authors_data)
        df_authors.to_excel(f"tiezi_authors_data_{self.file_index}.xlsx", index=False)
        self.logger.info(f"发言者数据已保存到 tiezi_authors_data_{self.file_index}.xlsx 文件中")
        self.authors_data = []  # 清空数据列表
        self.file_index += 1

    def close(self, reason):
        # 保存剩余未达到100行的数据
        if self.authors_data:
            self.save_authors_data()

        self.logger.info("爬取结束，所有数据已保存")
