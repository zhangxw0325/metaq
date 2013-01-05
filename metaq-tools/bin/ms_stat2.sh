#!/bin/bash
export LC_ALL=zh_CN.GBK
export PATH=$PATH:/opt/taobao/java/bin
base="/home/pingwei/stat2"
rm -rf $base/result.txt
rm -rf $base/*.info
while read line
do
	m1=`echo $line | awk -F ";" '{print $1}'`
	m2=`echo $line | awk -F ";" '{print $2}'`
	echo "fetch $m1 data..."
	ssh $m1 /bin/sh <<EOF >$base/$m1.info
	sh list2.sh
EOF
	echo "fetch $m2 data..."
	ssh $m2 /bin/sh <<EOF > $base/$m2.info
	sh list2.sh
EOF
	sort $base/$m1.info -k 2 -o  $base/$m1.info
	sort $base/$m2.info -k 2 -o  $base/$m2.info
	##mv $base/$m1.info.sort $base/$m1.info
	##mv $base/$m2.info.sort $base/$m2.info
	unset arr1
	unset arr2
	unset i1
	unset i2
	while read l1
	do
		arr1[$i1]=$l1
		let "i1=i1+1"
	done < $base/$m1.info
	while read l2
	do
		arr2[$i2]=$l2
		let "i2=i2+1"
	done < $base/$m2.info
	for((i=0; i< ${#arr1[@]}; i++))
	do
		if echo ${arr1[$i]} | cut -d "." -f2 | grep "meta" >> /dev/null
		then
			##value=`echo ${arr1[$i]} | awk -F "/" '{print $5}'`
			filepath=`echo ${arr1[$i]} | awk -F " " '{print $2}'`
			filename=`echo $filepath | awk -F "/" '{print $4}'`
			start_offset=$filename
			size=`echo ${arr1[$i]} | awk -F " " '{print $1}'`
			size=`expr $size \* 1024`
			offset1=0
			offset1=`expr $start_offset + $size`
			filepath=`echo ${arr2[$i]} | awk -F " " '{print $2}'`
                        filename=`echo $filepath | awk -F "/" '{print $4}'`
                        start_offset=$filename
                        size=`echo ${arr2[$i]} | awk -F " " '{print $1}'`
			size=`expr $size \* 1024`
                        offset2=0
                        offset2=`expr $start_offset + $size`
			diff=0
			diff=`expr $offset1 - $offset2`
			diff=`expr $diff / 1024 / 1024`
			diff_tmp=$diff
			if [ $diff -lt 0 ]
			then
			diff_tmp=`expr -1 \* $diff`
			fi
			max_diff=`cat $base/conf/diff.txt`
			exp=0
			let "exp=max_diff - diff_tmp"
			if [[ $exp -le 0 ]]
			then
			echo "machines["$m1, $m2"],diff["$diff,$max_diff"]" >> $base/result.txt
			fi
		fi
	done
		
done < $base/conf/machine_list.txt

date=`date '+%Y%m'`
mv $base/*.info $base/logs/
echo "[" `date` "]" >> $base/logs/monitor.$date.log
for i in `find $base/logs/*.info -type f`
do
echo $i >> $base/logs/monitor.$date.log
cat $i >> $base/logs/monitor.$date.log
done
wangwang=`cat $base/conf/wangwang.txt`
/opt/taobao/java/bin/java -Djava.ext.dirs=$base/libs/ -cp $base:$base/libs com.taobao.jm.metaq.Wangwang $base/result.txt $wangwang >> $base/logs/monitor.$date.log