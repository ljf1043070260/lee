from car import Car

'''
    此类是车主类，属性为车主名字，所属车列表，车主具体信息
    方法有属性需要的get，set方法，车主购物方法
'''
class CarUser():

    def __init__(self,name = "default",car = Car(),info = "default"):
        self.__name = name
        self.__carlist = [car]
        self.__info = info

    def set_carlist(self,car = Car()):
        self.__carlist.append(car) 

    def get_name(self):
        return self.__name

    def get_carlist(self):
        return self.__carlist

    def get_info(self):
        return self.__info
    
    def get_car_byid(self,id):
        for x in self.__carlist:
            if x.get_id() == id:
                return x
            
    def shopping(self):
        print("shopping........") 
        print("shopping........")    
        print("shopping........") 
        print("shopping........") 
        print("shopping........") 
        print("shopping........") 
        print("shopping........") 
        print("shopping........")     
    
    