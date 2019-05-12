import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api import VkUpload
import requests
from PIL import Image
from random import randint
import qrcode
import os
from multiprocessing import Process, Queue
from time import sleep
import time


token = ''
login, password = '', ''

class Vk_qr:
    def __init__(self):
        pass

    def do_qr_code(self, q):
        #получаем путь текущего каталога
        path = os.getcwd()                 
        path2 = path + '\codes'

        #если не существует каталога для кодов, создадим его
        if not os.path.exists(path2):      
            os.mkdir(path2)

        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=13,
            border=4,)

        qr.add_data('something')
        qr.make(fit=True)

        img = qr.make_image()
        way = 'codes\qr ' + str(randint(1, 999999)) + '.png'
        img.save(way)

        img_code = Image.open(way).convert("RGBA")
        img_pat = Image.open('photo.png')
        
        # центрирование изображения
        bwidth, bheight = img_pat.size[0],img_pat.size[1]
        fwidth, fheight = img_code.size[0],img_code.size[1]
        x,y = int((bwidth / 2)  - (fwidth / 2)), int((bheight / 2) - (fheight / 2))  

        # соединяем qr-код и шаблон
        img_pat.paste(img_code, (x, y), img_code)              
        new_way = 'codes\qr' + str(randint(1, 999999)) + '.png'
        img_pat.save(new_way)

        os.remove(way)
        q.put(new_way)
        return new_way
    

    def mess(self, q):
        vk = vk_api.VkApi(login, password)
        vk.auth(token_only=True)

        vk2 = vk_api.VkApi(token=token)

        vk_session2 = vk2.get_api()
        # vk_session = vk.get_api()
        
        longpoll = VkLongPoll(vk2)
        upload = VkUpload(vk)

        while True:
            attachments = []

            name = q.get()

            photo = upload.photo(name, album_id='', group_id='')
            attachments.append('photo{}_{}'.format(photo[0]['owner_id'],photo[0]['id']))

            vk_session2.messages.send(user_id='', 
                                                attachment=','.join(attachments), 
                                                message='Ваш код', 
                                                random_id=randint(1, 999999))
            sleep(3)


to = Vk_qr()

def main(q):
    while True:
        to.do_qr_code(q)
        sleep(0.7)


if __name__ == '__main__': 
    q = Queue()
    for i in range(7):
        proc = Process(target=main, args=(q, ))
        proc.start()
    to.mess(q)



