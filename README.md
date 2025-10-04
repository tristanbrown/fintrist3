# python-template
A template for personal Python projects.

## Initialization Instructions
1. Create project from template.
2. Change all instances of `python-template` to project name (including `src/python-template/`)
3. Copy `Dockerfile` and `.github/workflows` as necessary, for Docker image build on push of tagged version.
4. Change `docker-compose.yml` file to base on the new image.
5. Log into dockerhub on the host machine for image pull.
    - Use CLI access token as password.
    - If "error getting credentials," then `vim ~/.docker/config.json` and change the password store from `"desktop"` to `""`. 
6. Use `docker-compose up -d` to start the workspace.
