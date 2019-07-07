iter=0;
while [ $iter -le 10 ];
do
    ((iter++));
    if go test -run 2C >> 2C ; then
        echo "Pass ${iter}-th iteration"
    else
        echo "Fail ${iter}-th iteration"
    fi
done;