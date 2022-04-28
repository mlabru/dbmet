###
# configs
TEST_TARGET =
POETRY = poetry run

# autopep8 flags
AUTOPEP8_FLAGS = --diff --recursive --max-line-length 96 
# black flags
BLACK_FLAGS = -l 96 --check --diff

# original
FLAKE8_FLAGS0 = --ignore=W503,E501
# stop the build if there are Python syntax errors or undefined names
FLAKE8_FLAGS1 = --count --select=E9,F63,F7,F82 --show-source --statistics
# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
FLAKE8_FLAGS2 = --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics


ISORT_FLAGS = --profile=black --lines-after-import=2

###
## @ instalacao
.PHONY: instalar

instalar: ## instala programa usando poetry, poetry precisa estar instalado
	poetry install

###
## @ testes
.PHONY: teste cover

teste: ## roda teste
	${POETRY} pytest -v ${TEST_TARGET}

cover: ## roda testes de cobertura
	${POETRY} pytest -v --cov=. --cov-report=html ${TEST_TARGET}

###
## @ analise
.PHONY: autopep8 lint_black flake mypy lint_isort analise

autopep8:
	${POETRY} autopep8 ${AUTOPEP8_FLAGS} .

lint_black:
	${POETRY} black ${BLACK_FLAGS} .

flake:
	${POETRY} flake8 ${FLAKE8_FLAGS1} ./coords
	${POETRY} flake8 ${FLAKE8_FLAGS2} ./coords

mypy:
	${POETRY} mypy .

lint_isort:
	${POETRY} isort ${ISORT_FLAGS} --check .

analise: flake ## roda analise estatica: black, flake, mypy e isort
# autopep8 lint_black mypy lint_isort

###
## @ formatacao
.PHONY: black isort formatar

black:
	${POETRY} black .
isort:
	${POETRY} isort ${ISORT_FLAGS} .

formatar: isort black ## roda formatacao nos arquivos da pasta usando black e isort

###
## @ ajuda
.PHONY: ajuda

ajuda:
	@${POETRY} python ajuda.py

# < the end >----------------------------------------------------------------------------------
