import ftplib
import os
import zipfile
import mysql.connector as mysql

db = mysql.connect(user='root',
                   password='root',
                   host='127.0.0.1',
                   database='mydb')


def unzip():
    cursor = db.cursor()
    file_list = os.listdir('data')
    need_file = ('fcsProtocolIZP',
                 'fcsProtocolEFSingleApp',
                 'fcsProtocolEF3',
                 'fcsProtocolPPI',
                 'fcsNotificationEA44',
                 'fcsNotificationZP',
                 'fcsNotificationCancel',
                 'fcsPlacementResult')
    for zip_arch in file_list:
        if zip_arch.endswith('.zip'):
            try:
                print('unzip', zip_arch)
                zipfile.ZipFile('data' + '/' +
                                zip_arch).extractall('data/extract')
                sql = """INSERT INTO mydb.files
                            VALUES('%s', NOW())""" % (zip_arch)
                cursor.execute(sql)
            except Exception:
                pass

            os.remove('data' + '//' + zip_arch)
    for file in os.listdir('data/extract'):
        if file.upper().endswith('.SIG') or not file.startswith(need_file):
            print('remove', file)
            os.remove('data/extract' + '/' + file)
    db.commit()
    db.close()


def check_filename(filename_ftp):
    cursor = db.cursor()
    sql = 'SELECT file_name FROM mydb.files'
    cursor.execute(sql)
    res = {i for a in cursor.fetchall() for i in a}
    return filename_ftp - res


def ftp_download(region, folder):
    ftp = ftplib.FTP('ftp.zakupki.gov.ru', 'free', 'free')
    ftp.cwd('/fcs_regions/%s/%s' % (region, folder))
    file_ftp = ftp.nlst()

    for filename in check_filename(set(file_ftp)):
        print(f'DOWNLOAD: {filename}')
        host_file = os.path.join('data', filename)

        try:
            with open(host_file, 'wb') as local_file:
                ftp.retrbinary('RETR ' + filename, local_file.write, 1024)
        except Exception:
            pass
        local_file.close()
    ftp.quit()


if __name__ == '__main__':
    reg = ['Sevastopol_g', 'Krim_Resp']
    ftp_folder = ['protocols/currMonth',
                  'notifications/currMonth']
    for i in reg:
        for b in ftp_folder:
            try:
                ftp_download(i, b)
            except Exception as e:
                print(e)
    unzip()
