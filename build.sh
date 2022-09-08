datetime_str=`date +"%Y%m%d"`
image_name='ztrusdocker/aia_tools:'$datetime_str 
docker build  -f Dockerfile -t $image_name .