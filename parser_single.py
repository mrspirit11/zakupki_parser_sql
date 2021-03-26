from lxml import objectify, etree as et
import os
import mysql.connector as mysql

protocol_type = 'fcsProtocolEFSingleApp'
ns = '{http://zakupki.gov.ru/oos/types/1}'
ns3 = '{http://zakupki.gov.ru/oos/common/1}'

db = mysql.connect(
    user='root',
    password='root',
    host='127.0.0.1',
    database='mydb')
cursor = db.cursor()


def main(file_list):
    file_list = [file for file in file_list if file.startswith(protocol_type)]
    for file in file_list:
        print(file)
        ip_inn, ip_name, ip_contactEMail, ip_contactPhone = '', '', '', ''
        page = et.parse('data/extract/' + file)
        root = page.getroot()
        po_regNum = root.find(f'.//{ns}publisherOrg/{ns}regNum').text
        po_consRegistryNum = root.find(
            f'.//{ns}publisherOrg/{ns}consRegistryNum').text
        po_fullName = root.find(f'.//{ns}publisherOrg/{ns}fullName').text
        if root.find(f'.//{ns3}legalEntityRFInfo') is not None:
            org_fullname = root.find(
                f'.//{ns3}legalEntityRFInfo/{ns3}fullName').text
            org_inn = root.find(
                f'.//{ns3}legalEntityRFInfo/{ns3}INN').text
            org_kpp = root.find(
                f'.//{ns3}legalEntityRFInfo/{ns3}KPP').text
            org_email = root.find(
                f'.//{ns3}legalEntityRFInfo//{ns3}contactEMail')
            org_email = org_email.text if org_email is not None else ''
            org_phone = root.find(
                f'.//{ns3}legalEntityRFInfo//{ns3}contactPhone').text
        elif root.find(f'.//{ns3}individualPersonRFInfo')is not None:
            ip_name = ' '.join(
                [i.text for i in root.findall(
                    f'.//{ns3}individualPersonRFInfo//{ns3}nameInfo/')])
            ip_inn = root.find(
                f'.//{ns3}individualPersonRFInfo//{ns3}INN').text
            ip_contactEMail = root.find(
                f'.//{ns3}individualPersonRFInfo//{ns3}contactEMail')
            if ip_contactEMail:
                ip_contactEMail = ip_contactEMail.text 
            ip_contactPhone = root.find(
                f'.//{ns3}individualPersonRFInfo//{ns3}contactPhone').text

        admited = 'true'
        winnerPrice = root.find(
            f'.//{ns}winnerPrice')
        if winnerPrice is not None:
            winnerPrice = winnerPrice.text
        publishDate = root.find(
            f'.//{ns}publishDate').text
        publishDate = ' '.join(publishDate[:publishDate.find('.')].split('T'))
        _id = root.find(f'.//{ns}id').text
        purchaseNumber = root.find(f'.//{ns}purchaseNumber').text
        url = root.find(f'.//{ns}printForm/{ns}url').text
        sql = """INSERT INTO publishorg (regNum, consRegistryNum, FullName) 
                VALUES ('{}','{}','{}')""".format(
            po_regNum, po_consRegistryNum, po_fullName)
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            print(e, '\n', sql)
        if ip_inn:
            sql = """INSERT INTO ooo (INN, fullName, contactEmail, contactPhone) 
                    VALUES ('{}','{}','{}','{}')""".format(
                ip_inn, ip_name, ip_contactEMail, ip_contactPhone)
        else:
            sql = """INSERT INTO ooo (INN, KPP, fullName, contactEmail, contactPhone) 
                    VALUES ('{}','{}','{}','{}','{}')""".format(
                org_inn, org_kpp, org_fullname, org_email, org_phone)
        try:
            cursor.execute(sql)
            db.commit()
        except Exception:
            pass

        sql = """INSERT INTO protocol_info (id, purchaseNumber, publishDate, url, regNum, type) 
                VALUES ('{}','{}','{}','{}','{}','{}')""".format(
            _id, purchaseNumber, publishDate, url, po_regNum, protocol_type)
        try:
            cursor.execute(sql)
            db.commit()
        except Exception:
            pass

        sql = """INSERT INTO protocol (id, INN, admitted, price) 
                VALUES ('{}','{}','{}','{}')""".format(
            _id, ip_inn if ip_inn else org_inn, admited, winnerPrice)
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            print(e, '\n', sql)
        os.remove('data/extract/' + file)
    db.close()


if __name__ == '__main__':
    main(os.listdir('./data/extract'))
