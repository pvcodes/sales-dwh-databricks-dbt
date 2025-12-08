from configparser import ConfigParser
import os

# BASE_DIR = os.path.join((os.getcwd() + os.pardir))
BASE_DIR = os.path.dirname(os.getcwd())
BASE_DIR = os.getcwd()
print(BASE_DIR, 13123)


def load_config(filename=f"{BASE_DIR}/config/database.ini",
                section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    # print(parser.)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception("Section {0} not found in the {1} file".format(
            section, filename))

    return config


if __name__ == "__main__":
    config = load_config()
    print(config)
