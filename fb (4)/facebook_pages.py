from fbb.db_con import *
from fbb.process import *

if __name__=="__main__":
    fb1=fb_pages()
    data=fb1.process()
    if data:
        db_con().db_save(data)
