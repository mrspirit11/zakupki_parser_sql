import mysql.connector as mysql

cur_date = 'CURDATE()-3'



def request_from_db(sql):
	db = mysql.connect(
    user='root',
    password='root',
    host='127.0.0.1',
    database='mydb')
	
	cursor = db.cursor(dictionary=True)

	cursor.execute(sql)
    ans = cursor.fetchall()

    db.close()
    return ans


def format_ans(text_from_sql):



