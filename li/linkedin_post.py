from Linkedin_post.processing import *
from Linkedin_post.db_connection import *

if __name__=="__main__":
    db_con = db_con()
    li_automation = Li_process()
    result = li_automation.post_id()
    if result:
       db_con.db_save(result)
    global_list = db_con.db_posts()
    post_data = li_automation.post_metric_count(global_list)
    db_con.db_save(post_data)

