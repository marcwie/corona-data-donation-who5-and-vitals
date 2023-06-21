to_execute=(
    '1.02-analyze_vitals_vs_survey.ipynb'
)

cd notebooks

for filename in ${to_execute[@]}
do
	echo $filename
	poetry run jupyter nbconvert --to=python --output=out $filename
	poetry run python out.py
done

rm out.py
