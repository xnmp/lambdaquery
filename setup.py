from setuptools import setup

setup(
    name='lambdaquery',
    version='0.1.11',
    description='Composable SQL in Pure Python',
    author='Chong Wang',
    author_email='chonw89@gmail.com',
    packages=['LambdaQuery'],
    license='MIT',
    # url=['https://github.com/xnmp/lambdaquery'],
    setup_requires=['lenses'],
    keywords='databases query sql orm',
    install_requires=['lenses'],
    python_requires='>=3.6'
)

# cd LambdaQuery/
# python setup.py sdist upload
# cd ..
# pip install lambdaquery --upgrade