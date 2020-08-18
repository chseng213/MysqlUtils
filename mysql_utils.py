import os
import configparser

import pymysql
from sshtunnel import SSHTunnelForwarder


class ReadConfig:
    def __init__(self):
        self.base_dir = os.path.dirname(__file__)

    def get_config(self):
        file_path = os.path.abspath(os.path.join(self.base_dir, 'config.cfg'))
        cfg = configparser.RawConfigParser()
        cfg.read(file_path)
        env = cfg.get("ENV", "env")
        sections = cfg.sections()
        dic = {}
        # mysql配置
        mysql = str(env) + '_DATABASE'
        if mysql in sections:
            mysql_host = cfg.get(mysql, 'host')
            mysql_port = int(cfg.get(mysql, 'port'))
            mysql_user = cfg.get(mysql, 'user')
            mysql_password = cfg.get(mysql, 'password')
            mysql_database = cfg.get(mysql, 'database')
            dic.update({'mysql': {'host': mysql_host, 'port': mysql_port, 'user': mysql_user,
                                  'password': mysql_password, 'database': mysql_database}})
        # Mongodb配置
        mongodb = str(env) + '_MONGODB'
        if mongodb in sections:
            mongodb_uri = cfg.get(mongodb, 'uri')
            mongodb_db = cfg.get(mongodb, 'db')
            mongodb_coll = cfg.get(mongodb, 'coll')
            dic.update({'mongodb': {'uri': mongodb_uri, 'db': mongodb_db, 'coll': mongodb_coll}})

        # Remote配置
        remote = str(env) + '_REMOTE'
        if remote in sections:
            ssh_host = cfg.get(remote, 'host')
            ssh_port = int(cfg.get(remote, 'port'))
            ssh_user = cfg.get(remote, 'user')
            ssh_password = cfg.get(remote, 'password')
            dic.update({'remote': {'host': ssh_host, 'port': ssh_port, 'user': ssh_user, 'password': ssh_password}})

        return dic


class MySql:
    def __init__(self, cfg_dic, cfg_name, ssh=0):
        self.cfg_dic = cfg_dic
        # print(cfg_dic)
        config = cfg_dic[cfg_name]
        self.host = config['host']
        self.port = int(config['port'])
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        if ssh:
            self.server, self.conn, self.cur = self.ssh_connect()
        else:
            self.conn, self.cur = self.connect()

    def ssh_connect(self):
        """
            设置远程访问ssh,出现网关错误是因为服务器密码错误
            :param : 配置文件dict
            :return: 服务器对象，MySQL游标和链接
            """
        ssh_host = self.cfg_dic['remote']['host']
        ssh_port = self.cfg_dic['remote']['port']
        ssh_user = self.cfg_dic['remote']['user']
        ssh_password = self.cfg_dic['remote']['password']
        # print(ssh_host, ssh_port, ssh_user, ssh_password, mysql_host, mysql_port)
        server = SSHTunnelForwarder(ssh_address_or_host=(ssh_host, ssh_port),
                                    ssh_username=ssh_user, ssh_password=ssh_password,
                                    remote_bind_address=(self.host, self.port))
        server.start()
        local_host = '127.0.0.1'
        local_port = server.local_bind_port
        conn = pymysql.connect(host=local_host, port=local_port, user=self.user, password=self.password,
                               database=self.database, charset="utf8mb4", use_unicode=True)
        cur = conn.cursor()
        return server, conn, cur

    def connect(self):
        conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, charset="utf8mb4", use_unicode=True)
        cur = conn.cursor()
        return conn, cur

    def conn_sql(self):
        return self.conn, self.cur

    def close(self):
        self.cur.close()
        self.conn.close()

    def ssh_close(self):
        self.cur.close()
        self.conn.close()
        self.server.close()

    def query(self, sql, many=0):
        if not many:
            self.cur.execute(sql)
            item = self.cur.fetchone()
        else:
            self.cur.execute(sql)
            item = self.cur.fetchall()
        return item

    def execute(self, sql, params, many=0):
        if not many:
            self.cur.execute(sql, params)
            self.conn.commit()
        else:
            self.cur.executemany(sql, params)
            self.conn.commit()

    @staticmethod
    def create_query_sql(table, fields_str, condition, order_by=None):
        """select a,b from table where a>1 and b<10 order by a desc"""
        sql = """SELECT {} FROM {} WHERE {}""".format(fields_str, table, condition)
        if order_by:
            sql = sql + f' ORDER BY {order_by}'
        return sql

    @staticmethod
    def create_insert_sql(table, insert_type, field_len, fields=None):
        """
        insert/replace into table (a,b) values ('%s', '%s')
        insert ignore into table (a,b) values ('%s', '%s')
        fields 为None或者列表
        """
        if isinstance(fields, list):
            sql = """{} INTO {} ({}) VALUES ({})""".format(insert_type, table, ','.join(fields),
                                                           ','.join(field_len * ['%s']))
        else:
            sql = """{} INTO {} VALUES ({})""".format(insert_type, table, ','.join(field_len * ['%s']))
        return sql

    @staticmethod
    def create_update_sql(table, update_fields, condition):
        """update table set a=%s where a=%s"""
        sql = """UPDATE {} SET {} WHERE {}""".format(table, ','.join([f'{each}=%s' for each in update_fields]),
                                                     condition)
        return sql

    @staticmethod
    def create_duplicate_update_sql(table, fields, field_len):
        """INSERT INTO `table` (`a`, `b`, `c`) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE `c`=`c`+1  更新 C=C+1
        INSERT INTO `table` (`a`, `b`, `c`) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE `c`=`values(`c`)  更新 C=C

        """
        sql = """INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}""".format(
            table, ','.join(fields), ','.join(['%s'] * field_len),
            ','.join([f'{each}=values({each})' for each in fields]))
        return sql


if __name__ == '__main__':
    cfg = ReadConfig()
    cfg_dic = cfg.get_config()
    mysql = MySql(cfg_dic, 'mysql')
    user_sql = mysql.create_insert_sql('users', 'REPLACE', 3)
    user = [1, 1568486, 254]
    mysql.execute(user_sql, user)
