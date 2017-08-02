spark-submit \
--class wc \
--master yarn \
--deploy-mode cluster \  # can be client for client mode
--conf spark.ui.port = 22222 
# -- jars #extra jars
# --packages #extra packages
wc_2.xxx.jar arg0 arg1
