## Contributing

You are more than welcome to make any contributions.
Please create Pull Request for any changes.

### Fork the Repository

* When contributing to the library it is recommended that you fork the repository into you own. See [Github's Fork Guide](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) for more information

* Create a branch from 'master' and implement your changes

* Raise a pull request(PR) back to the master branch of gocelery

* It is good practice to reference an issue number, or explain what your enhancement lends to the project in the PR comments

* Continuous Integration builds must pass on the PR

* Newly implemented features or bug fixes must be accompanied by tests

* To get the latest changes from the forked repository [setup the upstream master](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/configuring-a-remote-for-a-fork) and [sync](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork) often 

### Installing for Local Development

After the package is cloned locally, follow these steps to setup your environment

1. Run ```make build``` from the root of the project

### Running the tests

1. [Install Redis](https://redis.io/download) 
2. [Install RabbitMQ](https://www.rabbitmq.com/download.html)
3. Run both services on their default ports
    * Redis: 6379
    * Rabbit: 5672
3. Run ```make test``` from your command line