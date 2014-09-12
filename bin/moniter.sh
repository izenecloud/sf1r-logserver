x=0
echo "TIME   VIRT   MEM"
while [ $x -eq 0 ]
do
  TIMENOW=`date +%y.%m.%d-%T`
  VIRT=`ps aux|grep "\./LogServerProcess"|grep -v grep|awk '{print $5}'`
  MEM=`ps aux|grep "\./LogServerProcess"|grep -v grep | awk '{print $6}'`
  MINFO=$TIMENOW" "$VIRT" "$MEM
  echo $MINFO
  sleep 5
done
PROCESS_NUM=`ps -aux|grep "\./LogServerProcess"|grep -v grep|wc -l`
~        
