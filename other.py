from lxml import etree as et
import mysql.connector as mysql
import os

file_type = ('fcsNotificationCancel', 'fcsPlacementResult')




def parse_xml(files):
    db = mysql.connect(
        user='root',
        password='root',
        host='127.0.0.1',
        database='mydb')
    cursor = db.cursor()
    files = [item for item in files if item.startswith(file_type)]
    for xml_name in files:
        tag = '{http://zakupki.gov.ru/oos/types/1}'
        tree = et.parse('data/extract/' + xml_name)
        root = tree.getroot()
        pn = ''
        dn = ''
        ar = ''
        if xml_name.startswith('fcsNotificationCancel'):
            try:
                pn = root.find(f'.//{tag}purchaseNumber').text
                dn = root.find(f'.//{tag}docPublishDate').text.split('T')[0]
            except Exception:
                tag = '{http://zakupki.gov.ru/oos/EPtypes/1}'
                pn = root.find(f'.//{tag}purchaseNumber').text
                dn = root.find(f'.//{tag}docPublishDTInEIS').text.split('T')[0]

            try:
                sql = f"""UPDATE mydb.NotificationEA44
                        SET cancel_date = '{dn}'
                        WHERE (purchaseNumber = '{pn}')"""
                cursor.execute(sql)
                print('OK', xml_name)
                os.remove('data/extract' + '/' + xml_name)
            except Exception as e:
                print(e, xml_name)
        if xml_name.startswith('fcsPlacementResult'):
            try:
                pn = root.find(f'.//{tag}purchaseNumber').text
                ar = root.find(f'.//{tag}name').text
            except Exception:
                pass
            try:
                if ar != '':
                    sql = f"""UPDATE mydb.NotificationEA44
                            SET abandonedReason = '{ar}'
                            WHERE (purchaseNumber = '{pn}')"""
                    cursor.execute(sql)
                    print('OK', xml_name)
                    os.remove('data/extract' + '/' + xml_name)
                else:
                    print('DEL', xml_name)
                    os.remove('data/extract' + '/' + xml_name)
            except Exception as e:
                print(e, xml_name)
                os.remove('data/extract' + '/' + xml_name)
    db.commit()
    db.close()


if __name__ == '__main__':
    files = os.listdir('data/extract/')
    parse_xml(files)
