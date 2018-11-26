
'''
    此类是车类，属性为车牌，所属车主，车具体信息
    方法有属性需要的get方法
'''
class Car():
    def __init__(self,id = "default",username = "default",info = "default"):
        self.__id = id
        self.__username = username
        self.__info = info
    
    def get_id(self):
        return self.__id

    def get_username(self):
        return self.__username 

    def get_info(self):
        return self.__info

    


