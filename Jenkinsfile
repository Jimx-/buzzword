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
        stage('Clippy') {
            steps {
                sh "cargo clippy --all"
            }
        }
        stage('Rustfmt') {
            steps {
                sh "cargo fmt --all -- --write-mode diff"
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