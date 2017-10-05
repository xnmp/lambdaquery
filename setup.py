from setuptools import setup

setup(
    name='lambdaquery',    
    # This is the name of your PyPI-package.
    version='0.1',
    # Update the version number for new releases
    packages=['LambdaQuery'],#['do_notation','expr','functions','misc','query','reroute','sql'],
    setup_requires=['lenses'],
    install_requires=['lenses']
)
