# GitHub Actions для сборки и публикации пакета

## Настройка

### 1. Требуемые секреты и настройки

Для автоматической публикации в PyPI необходимо настроить **Trusted Publishing** (рекомендуется) или использовать API токен.

#### Вариант А: Trusted Publishing (рекомендуется)

1. Зайдите на [pypi.org](https://pypi.org/) → Account Settings → Add API token
2. Выберите "Add trusted publisher"
3. Укажите:
   - **Project name**: `spark-logical-plan-capture`
   - **Repository**: ваш репозиторий (например, `username/spark-logical-plan-capture`)
   - **Workflow name**: `publish.yml`
   - **Environment name**: `pypi` (опционально)

#### Вариант Б: API Token

1. Создайте токен на [pypi.org/manage/account/token/](https://pypi.org/manage/account/token/)
2. В репозитории GitHub перейдите в Settings → Secrets and variables → Actions
3. Добавьте секрет:
   - **Name**: `PYPI_API_TOKEN`
   - **Value**: `pypi-...` (ваш токен)

### 2. Создание окружения (опционально, но рекомендуется)

1. Перейдите в Settings → Environments
2. Создайте окружение с именем `pypi`
3. Настройте правила развертывания (например, требовать ревью для публикации)

## Использование

### Автоматическая публикация при создании релиза

```bash
# Создайте тег и релиз в GitHub
git tag v0.2.1
git push origin v0.2.1

# Затем создайте релиз через GitHub UI или CLI
gh release create v0.2.1 --title "Version 0.2.1" --notes "Release notes"
```

После публикации релиза workflow автоматически:
1. Соберет пакеты (wheel и sdist)
2. Опубликует их в PyPI

### Ручная публикация через workflow_dispatch

1. Перейдите в Actions → "Build and Publish to PyPI"
2. Нажмите "Run workflow"
3. Опционально укажите версию
4. Workflow соберет и опубликует пакет

## Тестирование сборки

Перед публикацией можно протестировать сборку локально:

```bash
# Установите зависимости
pip install build twine

# Соберите пакет
python -m build

# Проверьте собранные файлы
twine check dist/*

# (Опционально) Загрузите в TestPyPI
twine upload --repository testpypi dist/*
```

## CI Workflow

Файл `ci.yml` автоматически запускается при:
- Push в ветки `main`/`master`
- Pull request в эти ветки

CI проверяет:
- Сборку пакета для Python 3.8-3.12
- Корректность импорта модулей
- Наличие JAR файла
- Валидность дистрибутива через `twine check`

## Структура workflow файлов

```
.github/workflows/
├── ci.yml          # Непрерывная интеграция (тесты при PR/push)
└── publish.yml     # Публикация в PyPI (при релизе или вручную)
```

## Версионирование

Версия пакета указывается в `pyproject.toml`:

```toml
[project]
name = "spark-logical-plan-capture"
version = "0.2.1"  # Обновляйте эту версию перед релизом
```

Рекомендуется следовать [Semantic Versioning](https://semver.org/):
- MAJOR.MINOR.PATCH (например, 0.2.1)
