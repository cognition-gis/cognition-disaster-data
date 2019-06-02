from setuptools import setup, find_packages

with open('./requirements.txt') as reqs:
    requirements = [line.rstrip() for line in reqs]

setup(name="cognition_disaster_data",
      version='0.1',
      author='Jeff Albrecht',
      author_email='geospatialjeff@gmail.com',
      packages=find_packages(),
      install_requires = requirements,
      entry_points= {
          "console_scripts": [
              "cognition-disaster-data=disaster_data.scripts.cli:cognition_disaster_data"
          ]},
      include_package_data=True
      )