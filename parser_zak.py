from lxml import etree as et
import os
import mysql.connector as mysql

dir_patch = 'data'
protocol_type = ('fcsNotificationOKD', 'fcsNotificationOKU',
                 'fcsNotificationZK504', 'fcsNotificationZP')

key_list = ('epNotificationEOK',
            'epNotificationEOKD',
            'epNotificationEZP',
            'epNotificationEZK',
            'epNotificationEOKOU',
            'commonInfo',
            'responsibleOrgInfo',
            'collectingInfo',
            'maxPriceInfo',
            'applicationGuarantee',
            'contractGuarantee',
            'customer'
            )

def save_to_db(d_list, ta):

    for key in tuple(d_list.keys()):
        if d_list[key] in ('', ' ', '-'):
            del d_list[key]
    if 'explanation' in d_list.keys():
        d_list['admitted'] = 'false'
    key_list = ', '.join(d_list.keys())
    value_list = tuple(d_list.values())
    db = mysql.connect(
        user='root',
        password='root',
        host='127.0.0.1',
        database='mydb')
    cursor = db.cursor()
    if ta == 'NotificationEA44':
        try:
            query = f"""INSERT INTO {ta} ({key_list})
            VALUES (%s{',%s'*(len(value_list)-1)})"""
            cursor.execute(query, value_list)
        except Exception:
            u_set = ','.join(
                    [f"{key}='{val}'" for key, val in d_list.items()])
            query = f"""UPDATE {ta}
                    SET {u_set}
                    WHERE purchaseNumber = '{d_list["purchaseNumber"]}'"""

            cursor.execute(query)
    elif ta == 'publishorg':
        sql = 'SELECT regNUM, inn FROM mydb.publishorg'
        cursor.execute(sql)
        res = {i for a in cursor.fetchall() for i in a}
        if d_list['regNum'] in res:
            if d_list.get('INN', 0) and d_list['INN'] not in res:
                u_set = ','.join(
                    [f"{key}='{val}'" for key, val in d_list.items()])
                query = f"""UPDATE {ta}
                            SET {u_set}
                            WHERE (regNUM = {d_list['regNum']})"""
                cursor.execute(query)
        else:
            query = f"""INSERT INTO {ta} ({key_list})
            VALUES (%s{',%s'*(len(value_list)-1)})"""
            cursor.execute(query, value_list)
    db.commit()
    db.close()


def parse_xml(xml_name, key_list=key_list):
    tree = et.parse(dir_patch + '/extract/' + xml_name)
    root = tree.getroot()
    d_dict = []
    x = '_'

    def pars(root, some_dict):
        nonlocal x
        for i in root:
            try:
                a = i[0]
                x = str(i.tag).split('}')[1]
            except IndexError:
                a = 1
            if a == 1:
                some_dict.append({(x, str(i.tag).split('}')[1]): i.text})
            else:
                pars(i, some_dict)
        return some_dict

    pars(root, d_dict)
    ot = []
    for i in d_dict:
        for key, value in i.items():
            for k in key:
                if k in key_list:
                    ot.append({key: value})
    return ot


def parse_to_db(files):
    files = [item for item in files if item.startswith(protocol_type)]
    file_num = len(files)

    def fdate(date_text):
        return date_text.split('T')[0] + \
            ' ' + date_text.split('T')[1][:date_text.split('T')[1].find('+')]
    for f in files:
        notif_info_list = ('epNotificationEZP',
                           'collectingInfo',
                           'maxPriceInfo',
                           'commonInfo',
                           )
        org_list = []
        org = {'regNum': '',
               'consRegistryNum': '',
               'fullName': '',
               'postAddress': '',
               'factAddress': '',
               'INN': '',
               'KPP': ''}
        responsibleOrg = {}
        customer = {}
        notif_info = {'purchaseNumber': '',
                      'purchaseObjectInfo': '',
                      'docPublishDate': '',
                      'endDate': '',
                      'href': '',
                      'maxPrice': '',
                      'applicationGuarantee': '',
                      'contractGuarantee': ''}

        for i in parse_xml(f):
            if tuple(i)[0][0] == 'responsibleOrgInfo':
                if tuple(i)[0][1] in org.keys():
                    responsibleOrg[tuple(i)[0][1]] = i[tuple(i)[0]]
            if tuple(i)[0][0] == 'customer':
                if tuple(i)[0][1] in org.keys():
                    customer[tuple(i)[0][1]] = i[tuple(i)[0]]
            if tuple(i)[0][0] in notif_info_list:
                if tuple(i)[0][1] in notif_info.keys():
                    notif_info[tuple(i)[0][1]] = i[tuple(i)[0]].strip()
                elif tuple(i)[0][1] == 'publishDTInEIS':
                    notif_info['docPublishDate'] = i[tuple(i)[0]].strip()
                elif tuple(i)[0][1] == 'endDT':
                    notif_info['endDate'] = i[tuple(i)[0]].strip()
            if tuple(i)[0][0] == 'applicationGuarantee':
                if tuple(i)[0][1] == 'amount':
                    notif_info['applicationGuarantee'] = i[tuple(i)[0]]
            if tuple(i)[0][0] == 'contractGuarantee':
                if tuple(i)[0][1] == 'amount':
                    notif_info['contractGuarantee'] = i[tuple(i)[0]]

        if responsibleOrg['regNum'] != customer['regNum']:
            org_list.append(responsibleOrg)
            org_list.append(customer)

        else:
            org_list.append(responsibleOrg)

        notif_info['responsibleOrg_regNum'] = responsibleOrg['regNum']
        notif_info['customer_regNum'] = customer['regNum']

        notif_info['docPublishDate'] = fdate(notif_info['docPublishDate'])
        notif_info['endDate'] = fdate(notif_info['endDate'])
        try:
            for o in org_list:
                save_to_db(o, 'publishorg')
            save_to_db(notif_info, 'NotificationEA44')
            print(file_num, 'OK', f)
            file_num -= 1
            os.remove('data/extract' + '/' + f)
        except Exception as e:
            print(e, f)


if __name__ == '__main__':
    files = os.listdir(dir_patch + '//extract//')
    parse_to_db(files)
