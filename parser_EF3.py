import os
from lxml import etree as et
import mysql.connector as mysql

dir_patch = 'data'
protocol_type = 'fcsProtocolEF3'

key_list = ('fcsProtocolEF3',
            'printForm',
            'publisherOrg',
            'application',
            'nameInfo',
            'legalEntityRFInfo',
            'contactInfo',
            'contactPersonInfo',
            'protocolCommissionMember',
            'nsiRejectReason',
            'abandonedReason'
            )

def save_to_db(d_list, ta):

    for key in tuple(d_list.keys()):
        if d_list[key] in ('', ' ', '-'):
            del d_list[key]

    if 'explanation' in d_list.keys():
        d_list['admitted'] = 'false'
        del d_list['explanation']

    key_list = ', '.join(d_list.keys())
    value_list = tuple(d_list.values())
    db = mysql.connect(
        user='root',
        password='root',
        host='127.0.0.1',
        database='mydb')
    cursor = db.cursor()
    if ta == 'protocol_info':
        # print(d_list)
        sql = 'SELECT purchaseNumber, id  FROM mydb.protocol_info'
        cursor.execute(sql)
        prot_id = dict(a for a in cursor.fetchall())
        if d_list['id'] not in prot_id.values():
            if d_list['purchaseNumber'] in prot_id.keys():
                if d_list['id'] > prot_id[d_list['purchaseNumber']]:
                    u_set = ','.join(
                        [f"{key}='{val}'" for key, val in d_list.items()])
                    query = f"""UPDATE {ta}
                                SET {u_set}
                                WHERE (purchaseNumber = '{d_list['purchaseNumber']}')"""
                    cursor.execute(query)
            else:
                query = f"""INSERT INTO {ta} ({key_list})
                VALUES (%s{',%s'*(len(value_list)-1)})"""
                cursor.execute(query, value_list)

    elif ta == 'publishorg':
        # print(d_list)
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
    elif ta == 'ooo':
        if len(d_list['INN']) > 10:
            d_list['fullName'] = f"{d_list.pop('lastName','')} {d_list.pop('firstName','')} {d_list.pop('middleName','')}"
        try:
            query = f"""INSERT INTO {ta} ({key_list})
            VALUES (%s{',%s'*(len(value_list)-1)})"""
            cursor.execute(query, value_list)
        except Exception:
            u_set = ','.join(
                [f"{key}='{val}'" for key, val in d_list.items()])
            query = f"""UPDATE {ta}
                        SET {u_set}
                        WHERE (INN = {d_list['INN']})"""
            cursor.execute(query)


    elif ta == 'protocol':
        if d_list:
            sql = 'SELECT id  FROM mydb.protocol_info'
            cursor.execute(sql)
            prot_id = set(i for a in cursor.fetchall() for i in a)
            if d_list['id'] in prot_id:
                query = f"""INSERT INTO {ta} ({key_list})
                VALUES (%s{',%s'*(len(value_list)-1)})"""
                cursor.execute(query, value_list)

    db.commit()
    db.close()


def parse_xml(xml_name, key_list=key_list):
    tree = et.parse(dir_patch + '//extract//' + xml_name)
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
                some_dict.append({x: {str(i.tag).split('}')[1]: i.text}})
            else:
                pars(i, some_dict)
        return some_dict

    pars(root, d_dict)
    ot = []
    for i in d_dict:
        for key, value in i.items():
            if key in key_list:
                ot.append((key, value))
    return ot


def parse_to_db(files):
    files = [item for item in files if item.startswith(protocol_type)]
    file_num = len(files)
    for f in files:
        ooo_list = []
        prot_list = []
        org_list = {'regNum': '',
                    'consRegistryNum': '',
                    'fullName': ''}
        ooo = {'INN': '',
               'KPP': '',
               'fullName': '',
               'shortName': '',
               'firmName': '',
               'orgPostAddress': '',
               'orgFactAddress': '',
               'lastName': '',
               'firstName': '',
               'middleName': '',
               'contactEMail': '',
               'contactPhone': '',
               'isIP': ''}
        prot_info = {'id': '',
                     'purchaseNumber': '',
                     'publishDate': '',
                     'url': ''}
        prot = {'admitted': '',
                'appRating': '',
                'winnerPrice': '',
                'explanation': ''}
        for i in parse_xml(f):
            if i[0] == 'publisherOrg':
                for a, b in i[1].items():
                    if a in org_list.keys():
                        org_list[a] = b

            elif i[0] in key_list:
                for key, val in i[1].items():

                    if key in prot_info.keys():
                        prot_info[key] = val

                    elif key in ooo.keys():
                        if key == 'postAddress':
                            ooo['orgPostAddress'] = val
                        elif key == 'factAddress':
                            ooo['orgFactAddress'] = val
                        else:
                            ooo[key] = val.upper().strip().replace("'", '"')

                    elif key in prot.keys():
                        prot['INN'] = ooo['INN']
                        prot['id'] = prot_info['id']
                        prot[key] = val

            if 'increaseWinnerInitialPrice' in i[1].keys():
                ooo_list.append(ooo.copy())
                prot_list.append(prot.copy())
                ooo = {'INN': '',
                       'KPP': '',
                       'fullName': '',
                       'shortName': '',
                       'firmName': '',
                       'orgPostAddress': '',
                       'orgFactAddress': '',
                       'lastName': '',
                       'firstName': '',
                       'middleName': '',
                       'contactEMail': '',
                       'contactPhone': '',
                       'isIP': ''}
                prot = {'admitted': '',
                        'appRating': '',
                        'winnerPrice': '',
                        'explanation': ''}

        date_time = prot_info['publishDate'].split('T')
        prot_info['publishDate'] = date_time[0] + \
            ' ' + date_time[1][:date_time[1].find('.')]

        prot_info['regNum'], prot_info['type'] = org_list['regNum'], protocol_type
        try:
            save_to_db(prot_info, 'protocol_info')
            save_to_db(org_list, 'publishorg')
            for ooo_dict in ooo_list:
                if ooo_dict['isIP'] == 'TRUE':
                    ooo_dict['isIP'] = 1
                elif ooo_dict['isIP'] == 'FALSE':
                    ooo_dict['isIP'] = 0
                save_to_db(ooo_dict, 'ooo')
            for p_info in prot_list:
                p_info['price'] = p_info.pop('winnerPrice')
                save_to_db(p_info, 'protocol')
            print(file_num, 'OK', f)
            file_num -= 1
            os.remove('data/extract' + '/' + f)
        except Exception as e:
            print(e, f)


if __name__ == '__main__':
    files = os.listdir(dir_patch + '//extract//')
    parse_to_db(files)
