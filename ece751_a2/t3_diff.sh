
#!/bin/bash

OUTPUT_3=t3_in3.txt
OUTPUT_4=t3_in4.txt

cat $OUTPUT_3 | sort > normalized_3.txt
cat $OUTPUT_4 | sort > normalized_4.txt

echo Diffing 3 and 4 outputs:
diff normalized_3.txt normalized_4.txt

if [ $? -eq 0 ]; then
    echo Outputs match.
else
    echo Outputs do not match. Looks for bugs.
fi

