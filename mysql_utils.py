import os
import configparser


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


if __name__ == '__main__':
    rc = ReadConfig()
    print(rc.get_config())
