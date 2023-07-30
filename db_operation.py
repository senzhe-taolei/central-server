import pymysql


class MySqlHelper(object):
    def __init__(self, host="localhost"):
        if host == "localhost":
            ip = "localhost"
            password = "jinqilinuser"
            database = "if_central_control"
        else:
            ip = "192.168.10.137"
            password = "JinQiLin!@#$%^123456"
            database = "if_factory"
        self.db = pymysql.connect(host=ip, user="root", password=password, database=database,
                                  charset="utf8")
        # self.db = pymysql.connect(host="192.168.10.129", user="root", password="JinQiLin!@#$%^123456",
        #                           database="if_central_control", charset="utf8")
        self.cursor = self.db.cursor()

    def close_db(self):
        # 关闭数据库连接
        self.db.close()

    def insert_or_update_db_data(self, sql, param_list):
        # 使用 execute()  方法执行 SQL 查询
        self.cursor.execute(sql, param_list)
        insert_id = self.db.insert_id()
        self.db.commit()
        return insert_id

    # 改写数据库
    def update_db_data(self, sql):
        # 使用 execute()  方法执行 SQL 查询
        self.cursor.execute(sql)
        self.db.commit()

    # 数据库查询
    def get_db_data(self, get_data_sql):
        # 使用 execute()  方法执行 SQL 查询
        self.cursor.execute(get_data_sql)
        data = self.cursor.fetchall()
        return data

    # 获取查询结果的字段名和值的字典
    def get_db_data_dict(self, get_data_sql):
        self.cursor.execute(get_data_sql)
        field = self.cursor.description
        data = self.cursor.fetchall()
        field_list = []
        for i in field:
            field_list.append(i[0])
        data_dict = {}
        for k, v in enumerate(data[0]):
            data_dict[field_list[k]] = v
        return data_dict

    def write_log_no_task_number(self, log_name: str, log: str):
        insert_data_sql = "insert into sys_log(log_name, create_time, log ) values (%s, now(3), %s)"
        insert_param_list = [log_name, log]
        self.insert_or_update_db_data(insert_data_sql, insert_param_list)

    def write_log_have_task_number(self, log_name: str, wcs_task_number: str, log: str):
        insert_data_sql = "insert into sys_log(log_name, create_time, wcs_task_number, log ) " \
                          "values (%s, now(3), %s, %s)"
        insert_param_list = [log_name, wcs_task_number, log]
        self.insert_or_update_db_data(insert_data_sql, insert_param_list)
