class DbCollections(object):
    """This class implement collections in FFCS_DB that can be accessed by ffcsdbclient"""
    def __init__(self):
        self.__dict__['plates'] = 'Plates'
        self.__dict__['wells']  = 'Wells'
        self.__dict__['notifications'] = 'Notifications'
        self.__dict__['libraries'] = 'Libraries'
        self.__dict__['campaign_libraries'] = 'Campaign_Libraries'

    def __getitem__(self, item):
        return self.__dict__[item]
