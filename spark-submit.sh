spark-submit \
--class wc \
--master yarn \
--conf spark.ui.port = 22222 \
wc_2.xxx.jar arg0 arg1
