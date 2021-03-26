import os
import get_ftp
import parser_zak as zak
import parser_zak_fcsNotificationEA44 as zak44
import parser_EF3 as ef3
import parser_PPI as ppi
import parser_single as ps
import parser_IZP as izp
import other
# import mail


dir_patch = 'data'
reg = ['Sevastopol_g', 'Krim_Resp']
ftp_folder = ['protocols/currMonth',
              'notifications/currMonth']

for i in reg:
    for b in ftp_folder:
        try:
            get_ftp.ftp_download(i, b)
        except Exception as e:
            print(e)

get_ftp.unzip()


# mail.file()

files = os.listdir(dir_patch + '//extract//')

zak.parse_to_db(files)
files = os.listdir(dir_patch + '//extract//')
zak44.parse_to_db(files)
izp.parse_to_db(files)
ef3.parse_to_db(files)
ppi.parse_to_db(files)
ps.main(files)
other.parse_xml(files)
