from setuptools import setup

setup(
    name='lambdaquery',
    version='0.1',
    description='Composable SQL in Pure Python',
    author='Chong Wang',
    author_email='chonw89@gmail.com',
    packages=['LambdaQuery'],
    url=['https://github.com/xnmp/lambdaquery'],
    setup_requires=['lenses'],
    install_requires=['lenses']
)
