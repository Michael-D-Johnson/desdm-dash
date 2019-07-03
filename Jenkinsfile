node {
    def app

    stage('Clone repository') {
        /* Let's make sure we have the repository cloned to our workspace */

        checkout scm
    }

    stage('Build image') {
        /* This builds the actual image; synonymous to
         * docker build on the command line */

        app = docker.build("mdjohnson/desdm-dash")
    }

    ws("/var/jenkins_home/workspace/docker_container_test"){
    stage('Test image') {
        /* Ideally, we would run a test framework against our image.
         * For this example, we're using a Volkswagen-type approach ;-) */
        app.inside("-u 0:0") {
            sh 'echo "Tests passed"'
        }
    }
    }
    stage('Push image') {
        /* We'll push the image with two tags:
         * First, the incremental build number from Jenkins
         * Second, the 'latest' tag.
         * Pushing multiple tags is cheap, as all the layers are reused. */
        docker.withRegistry('https://registry.hub.docker.com', 'mdjohnson-docker-hub-credentials') {
            app.push("v${env.BUILD_NUMBER}")
            app.push("latest")
        }
    }

    stage('Deploy on kubernetes') {
        /* Finally, we'll deploy latest build on kubernetes */
        sh "kubectl set image deployment/desdm-dash desdm-dash=docker.io/mdjohnson/desdm-dash:v${env.BUILD_NUMBER}" 
        }
}
