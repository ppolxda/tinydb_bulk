pipenv run coverage run --source=tinydb_bulk setup.py test
pipenv run coverage html -d htmlcov
pipenv run coverage report
