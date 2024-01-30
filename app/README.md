# ffcs_db_server

ffcs_db_server is a python FastAPI server for interacting with mongoDB.

## Connection to FFCS DB Through ffcsdbclient

ffcs_db_server initiates an instance of the ffcs_db_utils class , which in turn
connects to the FFCS DB. To enable a seamless transition from the previously
used, now deprecated clien class (`ffcsdbclient`; not to be confused with
the new `ffcs_db_client` class), all of its functions were transferred to
the new ffcs_db_utils class. All methods of the new ffcs_db_client class
adress API endpoints in ffcs_db_server that, in turn, use methods from
ffcs_db_utils.

        ffcs_db_client <-> ffcs_db_server <-> ffcs_db_utils

All methods of ffcs_db_client require the same input as the eponymous
methods from ffcs_db_client to enable a seamless exange of the old client
(`ffcsdbclient`) by the new client (`ffcs_db_client`). For this purpose, also
the output format of ffcs_db_client should be identical to that of
ffcsdbclient.

However, some methods of ffcsdbclient return documents containing datetime
objects, MongoDB ObjectIDs, or MongoDB pointers, which cannot be transmitted
through FastAPI. Such objects and were converted to strings, transmitted
through FastAPI, and then converted back to the respective object or CursorMock
objects as appropriate.

## Updates from Deprecated Pre-MongoDB v3.2 Database Interactions

In the latest update, we have upgraded our database interaction functions to
align with the MongoDB v5.0 standards. This includes the utilization of
`update_one`, `update_many`, `insert_one`, `find_one`, `delete_one`, and other
similar methods. These methods offer enhanced clarity and functionality
compared to their predecessors.

To ensure backward compatibility and facilitate a seamles transition to the new
`ffcs_db_client` class, we have implemented a system where the result objects
returned by these new functions are converted to match the format and structure
of the pre-v3.2 specifications. This approach ensures that existing
applications integrating with our database can continue to operate without any
significant changes, while new applications can take full advantage of the
improvements in MongoDB's latest version.

Please note that while we strive to maintain backward compatibility, we
encourage users to update their code to utilize the new API methods for better
performance and future compatibility.

## Auxilliary Methods

A number of auxilliary functions was added to ffcsdbclient, which are mostly
used for the integration test in `ffcs_db_client_integration_test.py`.

        add_campaign_library: Adds a campaign library

        delete_by_id
        delete_by_query
        get_notifications
        get_one_campaign_library
        get_one_library
        print_update_result
        update_by_object_id_NEW

## Pydantic Base Models

Specific outputs and inputs of API endpoints in ffcs_db_server are defined
through pydantic base models. This was done on a case-to-case basis and
could be improved for consistency.

### Managing the fccs_db_server in Docker
## Stopping | Building | Starting

        cd /sls/MX/applications/git/ffcs/ffcs_db_server
        cd /home/smith_k/ffcs_db_server
        docker-compose down
        docker-compose build --no-cache
        docker-compose up ### with -d flag for running detached

## Dockerfile

        FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

        COPY ./app /app

        RUN pip install pymongo python-dateutil

        EXPOSE 8000

        #CMD ["uvicorn", "ffcs_db_server:app", "--host", "0.0.0.0", "--port", "8081"]
        CMD ["uvicorn", "ffcs_db_server:app", "--host", "0.0.0.0", "--port", "8081", "--ssl-keyfile", "/app/key.pem", "--ssl-certfile", "/app/cert.pem"]

### Knows Issues

### Git

        git pull
        git status
        git add . && git commit -a -m "Work in progress: converting ffcsdbclient functionality"
        git push
        git pull ; git status ; git add . && git commit -a -m "Canonicalized API endpoint url names" ; git push

## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://git.psi.ch/ffcs/ffcs_db_server.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](https://git.psi.ch/ffcs/ffcs_db_server/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Automatically merge when pipeline succeeds](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing(SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!).  Thank you to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README
Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.