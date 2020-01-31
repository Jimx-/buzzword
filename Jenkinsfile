pipeline {
    agent {
        docker {
            image 'rust:latest'
        }
    }
    
    stages {
        stage('Build') {
            steps {
                checkout scm
                sh "cargo build"
            }
        }
        stage('Test') {
            steps {
                sh "cargo test"
            }
        }
        stage('Doc') {
            steps {
                sh "cargo doc"
                step([$class: 'JavadocArchiver',
                      javadocDir: 'target/doc',
                      keepAll: false])
            }
        }
    }
}