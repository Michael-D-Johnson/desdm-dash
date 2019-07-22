node {
    def app
    env.BUILD_VERSION = divide_build_number()

    stage('Clone repository') {
        /* Let's make sure we have the repository cloned to our workspace */

        checkout scm
    }

    stage('Build image') {
        /* This builds the actual image; synonymous to
         * docker build on the command line */

        app = docker.build("mdjohnson/desdm-dash")
    }

    stage('Test image') {
        /* Ideally, we would run a test framework against our image.
         * For this example, we're using a Volkswagen-type approach ;-) */
        app.inside {
            sh 'echo "Tests passed"'
        }
    }

    stage('Push image') {
        /* We'll push the image with two tags:
         * First, the incremental build number from Jenkins
         * Second, the 'latest' tag.
         * Pushing multiple tags is cheap, as all the layers are reused. */
        docker.withRegistry('https://registry.hub.docker.com', 'mdjohnson-docker-hub-credentials') {
            app.push("v${env.BUILD_VERSION}")
            app.push("latest")
        }
    }

    stage('Deploy on kubernetes') {
        /* Finally, we'll deploy latest build on kubernetes */
        sh "kubectl set image -n deslabs deployment/desdm-dash desdm-dash=docker.io/mdjohnson/desdm-dash:v${env.BUILD_VERSION}" 
        }

    stage('Clean up unused docker builds') {
        sh "docker system prune -a -f"
    }
}

def divide_build_number(){
    node {
        build_version = env.BUILD_NUMBER.toLong() / 100.0
        build_version_float = build_version.toFloat()
        return String.format("%.2f",build_version_float)
    }
}
