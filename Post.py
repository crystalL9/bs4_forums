class PostForumz:
    def __init__(self, **kwargs):
        self.id = kwargs.get('id', '')
        self.type = kwargs.get('type', '')
        self.time_crawl = kwargs.get('time_crawl', '')
        self.link = kwargs.get('link', '')
        self.author = kwargs.get('author', '')
        self.author_link = kwargs.get('author_link', '')
        self.id_user = kwargs.get('id_user', '')
        self.avatar = kwargs.get('avatar', '')
        self.created_time = kwargs.get('created_time', '')
        self.content = kwargs.get('content', '')
        self.role = kwargs.get('role', '')
        self.image_url = kwargs.get('image_url', [])
        self.like = kwargs.get('like', 0)
        self.comment = kwargs.get('comment', 0)
        self.list_like = kwargs.get('list_like', [])
        self.wow = kwargs.get('wow', 0)
        self.list_wow = kwargs.get('list_wow', [])
        self.haha = kwargs.get('haha', 0)
        self.list_haha = kwargs.get('list_haha', [])
        self.love = kwargs.get('love', 0)
        self.list_love = kwargs.get('list_love', [])
        self.angry = kwargs.get('angry', 0)
        self.list_angry = kwargs.get('list_angry', [])
        self.domain = kwargs.get('domain', '')
        self.hashtag =  kwargs.get('hashtag', [])
        self.title = kwargs.get('title', '')
        self.view = kwargs.get('view', 0)
        self.video = kwargs.get('video', [])
        self.source_id = kwargs.get('source_id', '')
        self.out_links = kwargs.get('out_links', [])

    def is_valid(self) -> bool:
        is_valid = self.id != "" and self.author != "" and self.link != "" and self.created_time 
        return is_valid

    def __str__(self) -> str:
        string = ""
        for attr_name, attr_value in self.__dict__.items():
            string =  f"{attr_name}={attr_value}\n" + string
        return string
        