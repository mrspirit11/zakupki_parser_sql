import mysql.connector as mysql

db = mysql.connect(
    user='root',
    password='root',
    host='127.0.0.1',
    database='mydb')
cursor = db.cursor(dictionary=True)

cur_date = 'CURDATE()-3'


def file():
    sql = '''SELECT obj_name, url, №, DATE_format(p_date, '%d-%m-%Y %T') as publ_date, 
                    maxprice, cont_Guarant, contract_pers, rating, price, 
                    org_name, org_inn, email, phone 
             FROM mydb.winner 
             WHERE p_date > {}
             AND (rating < 2 or  (rating is NULL and admitted = 'true'))
             AND maxPrice >= '30000000' 
             ORDER BY p_date DESC
             '''.format(cur_date)
    cursor.execute(sql)
    ans = cursor.fetchall()
    s = ''
    for i in ans:
        if i['maxPrice']:
            i['maxPrice'] = '{:,}'.format(i['maxPrice']).replace(',', ' ')
        if i['cont_Guarant']:
            i['cont_Guarant'] = '{:,}'.format(
                i['cont_Guarant']).replace(',', ' ')
        else:
            i['cont_Guarant'] = ' - '
        if i['price']:
            i['price'] = '{:,}'.format(i['price']).replace(',', ' ')
        if i['contract_pers'] is not None:
            pers = f"{i['contract_pers']*100}%"
        else:
            pers = ''
        s += f"""<a href="{i['url']}">{i['obj_name']}</a><br>{i['№']}<br><br>
    <table><tbody>
    <tr><td>Дата публикации:<br></td><td>{i["publ_date"]}</td></tr>
    <tr><td>НМЦК:<br></td><td>{i['maxPrice']} руб.</td></tr><tr>
    <td>Обеспечение контракта:</td><td><b>{i['cont_Guarant']} руб. {pers}</td></tr>
    </tbody></table>
    <br>
    <table><tbody>
    <tr><td>Предл. цена:</td><td>{i['price']} руб.</td></tr>
    <tr><td>Наименование :</td><td>{i['org_name']}</td></tr>
    <tr><td>ИНН :</td><td>{i['org_inn']}</td></tr>
    <tr><td>Телефон :</td><td>{i['phone']}</td></tr>
    <tr><td>Email: </td><td>{i['email']}</td></tr>
    </tbody></table>
    <br>{'-' * 40}<br><br><br>"""

    with open('C:\\Users\\mrspi\\Desktop\\win_all.html', 'w') as f:
        f.write(s)
    print('OK', f.name)

    sql = '''SELECT purchaseNumber, purchaseObjectInfo, 
                     DATE_format(docPublishDate, '%d-%m-%Y %T') AS publ_date, 
                     DATE_format(endDate, '%d-%m-%Y %T') AS end_date, 
                     href, maxPrice, app_Guarant, cont_Guarant, contract_pers, name, INN  
              FROM mydb.miac
              WHERE docPublishDate > {}'''.format(cur_date)
    cursor.execute(sql)
    ans = cursor.fetchall()
    s = ''
    for i in ans:
        if i['maxPrice']:
            i['maxPrice'] = '{:,}'.format(i['maxPrice']).replace(',', ' ')
        else:
            i['maxPrice'] = ' - '
        if i['cont_Guarant']:
            i['cont_Guarant'] = '{:,}'.format(
                i['cont_Guarant']).replace(',', ' ')
        else:
            i['cont_Guarant'] = ' - '
        if i['app_Guarant']:
            i['app_Guarant'] = '{:,}'.format(
                i['app_Guarant']).replace(',', ' ')
        else:
            i['app_Guarant'] = ' - '
        if i['contract_pers'] is not None:
            pers = f"{i['contract_pers']*100}%"
        else:
            pers = ''
        s += f"""<a href="{i['href']}">{i['purchaseObjectInfo']}</a><br>{i['purchaseNumber']}<br><br>
    <table><tbody>
    <tr><td>Дата публикации:<br></td><td>{i["publ_date"]}</td></tr>
    <tr><td>Дата окончания подачи:<br></td><td>{i["end_date"]}</td></tr>
    <tr><td>НМЦК:<br></td><td>{i['maxPrice']} руб.</td></tr><tr>
    <td>Обеспечение контракта:</td><td><b>{i['cont_Guarant']} руб. {pers}</td></tr>
    <tr><td>Обеспечение заявки:</td><td>{i['app_Guarant']} руб.<br></td></tr>
    </tbody></table>
    <br>{i['name']}
    <br>{i['INN']}
    <br>{'-' * 40}<br><br><br>"""

    with open('C:\\Users\\mrspi\\Desktop\\miac.html', 'w') as f:
        f.write(s)
    print('OK', f.name)

    sql = '''SELECT purchaseNumber, purchaseObjectInfo, 
                    DATE_format(docPublishDate, '%d-%m-%Y %T') AS publ_date, 
                    DATE_format(endDate, '%d-%m-%Y %T') AS end_date, 
                    href, maxPrice, app_Guarant, cont_Guarant, contract_pers, name, INN  
            FROM mydb.notification
            WHERE docPublishDate > {}
            AND maxPrice >= '300000000'
    '''.format(cur_date)
    cursor.execute(sql)
    ans = cursor.fetchall()
    s = ''
    for i in ans:
        if i['maxPrice']:
            i['maxPrice'] = '{:,}'.format(i['maxPrice']).replace(',', ' ')
        else:
            i['maxPrice'] = ' - '
        if i['cont_Guarant']:
            i['cont_Guarant'] = '{:,}'.format(
                i['cont_Guarant']).replace(',', ' ')
        else:
            i['cont_Guarant'] = ' - '
        if i['app_Guarant']:
            i['app_Guarant'] = '{:,}'.format(
                i['app_Guarant']).replace(',', ' ')
        else:
            i['app_Guarant'] = ' - '
        if i['contract_pers'] is not None:
            pers = f"{i['contract_pers']*100}%"
        else:
            pers = ''
        s += f"""<a href="{i['href']}">{i['purchaseObjectInfo']}</a><br>{i['purchaseNumber']}<br><br>
    <table><tbody>
    <tr><td>Дата публикации:<br></td><td>{i["publ_date"]}</td></tr>
    <tr><td>НМЦК:<br></td><td>{i['maxPrice']} руб.</td></tr><tr>
    <td>Обеспечение контракта:</td><td><b>{i['cont_Guarant']} руб. {pers}</td></tr>
    <tr><td>Обеспечение заявки:</td><td>{i['app_Guarant']} руб.<br></td></tr>
    </tbody></table>
    <br>{i['name']}
    <br>{i['INN']}
    <br>{'-'*40}<br><br><br>"""

    with open('C:\\Users\\mrspi\\Desktop\\big.html', 'w') as f:
        f.write(s)
    print('OK', f.name)

    sql = '''SELECT obj_name, url, №, DATE_format(p_date, '%d-%m-%Y %T') as publ_date, 
                    maxprice, cont_Guarant, contract_pers, rating, price, 
                    org_name, org_inn, email, phone 
             FROM mydb.winner_miac
            WHERE (rating < 2 or rating is Null)
            AND p_date > {}
                '''.format(cur_date)
    cursor.execute(sql)
    ans = cursor.fetchall()
    s = ''
    for i in ans:
        if i['maxPrice']:
            i['maxPrice'] = '{:,}'.format(i['maxPrice']).replace(',', ' ')
        if i['cont_Guarant']:
            i['cont_Guarant'] = '{:,}'.format(
                i['cont_Guarant']).replace(',', ' ')
        else:
            i['cont_Guarant'] = ' - '
        if i['price']:
            i['price'] = '{:,}'.format(i['price']).replace(',', ' ')
        if i['contract_pers'] is not None:
            pers = f"{i['contract_pers']*100}%"
        else:
            pers = ''
        s += f"""<a href="{i['url']}">{i['obj_name']}</a><br>{i['№']}<br><br>
    <table><tbody>
    <tr><td>Дата публикации:<br></td><td>{i["publ_date"]}</td></tr>
    <tr><td>НМЦК:<br></td><td>{i['maxPrice']} руб.</td></tr><tr>
    <td>Обеспечение контракта:</td><td><b>{i['cont_Guarant']} руб. {pers}</td></tr>
    </tbody></table>
    <br>
    <table><tbody>
    <tr><td>Предл. цена:</td><td>{i['price']} руб.</td></tr>
    <tr><td>Наименование :</td><td>{i['org_name']}</td></tr>
    <tr><td>ИНН :</td><td>{i['org_inn']}</td></tr>
    <tr><td>Телефон :</td><td>{i['phone']}</td></tr>
    <tr><td>Email: </td><td>{i['email']}</td></tr>
    </tbody></table>
    <br>{'-'*40}<br><br><br>"""

    with open('C:\\Users\\mrspi\\Desktop\\win_miac.html', 'w') as f:
        f.write(s)
    print('OK', f.name)


if __name__ == '__main__':
    file()
