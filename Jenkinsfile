project = "do-ess-data-simulator"

centos = 'essdmscdm/centos.python-build-node:0.1.2'

container_name = "${project}-${env.BRANCH_NAME}-${env.BUILD_NUMBER}"

node("docker") {
    cleanWs()
    dir("${project}") {
        stage("Checkout") {
            scm_vars = checkout scm
        }
    }
    try {
        image = docker.image(centos)
            container = image.run("\
                --name ${container_name} \
                --tty \
                --network=host \
                --env http_proxy=${env.http_proxy} \
                --env https_proxy=${env.https_proxy} \
            ")
        sh "docker cp ${project} ${container_name}:/home/jenkins/${project}"
        sh """docker exec --user root ${container_name} bash -c \"
            chown -R jenkins.jenkins /home/jenkins/${project}
        \""""
        
        stage("Install requirements") {
            sh """docker exec ${container_name} bash -c \"
                cd ${project}
                build_env/bin/pip --proxy ${http_proxy} install --upgrade pip
                build_env/bin/pip --proxy ${http_proxy} install -r requirements.txt --user
            \""""
        }
        
    } finally {
        container.stop()
    }
}