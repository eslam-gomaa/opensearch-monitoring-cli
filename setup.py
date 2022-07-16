from setuptools import setup, find_packages
import os

requirements = 'requirements.txt'
if os.path.isfile(requirements):
    with open(requirements) as f:
        install_requires = f.read().splitlines()

setup(
    name = 'opmcli',
    version = '0.0.1',
    author = 'Eslam Gomaa',
    # license = '<the license you chose>',
    description = 'A CLI tool for opensearch performance monitoring',
    long_description_content_type = "text/markdown",
    url = 'https://github.com/eslam-gomaa/opcli',
    py_modules = ['opmcli', 'opmcli_tool'],
    packages = find_packages(),
    install_requires = [install_requires],
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
    # entry_points = '''
    #     [console_scripts]
    #     opmcli=opmcli_tool:cli
    # '''
    entry_points={
        'console_scripts':['opmcli = opmcli_tool:run']
   }
)

    