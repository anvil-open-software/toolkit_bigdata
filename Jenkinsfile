#!groovy​
properties([
        buildDiscarder(logRotator(numToKeepStr: '5')),
        gitLabConnection('gitlab')
])

timestamps {
    def currentProjectVersion
    try {
        parallel( // provided that two builds can actually run at the same time without conflicts...
                'build': {

                    node {
                        gitlabCommitStatus('build') {
                            ansiColor('xterm') {
                                stage('checkout') {
                                    checkout scm
                                    sh 'git clean -dfx && git reset --hard'

                                    currentProjectVersion = readMavenPom().version
                                }

                                stage('build') {
                                    maven('-U', currentProjectVersion, '-Snapshot')
                                }
                            }
                        }
                    }
                },
                'sonar': {
                    if (branchProhibitsSonar()) {
                        echo "Skipping sonar tests on ${env.BRANCH_NAME}"
                        return
                    }
                    node {
                        gitlabCommitStatus('sonar') {
                            ansiColor('xterm') {
                                stage('checkout') {
                                    checkout scm
                                    sh 'git clean -dfx && git reset --hard'
                                }

                                stage('SonarQube analysis') {
                                    withSonarQubeEnv('dlabs') {
                                        maven("clean verify ${env.SONAR_MAVEN_GOAL} -Dsonar.host.url=${env.SONAR_HOST_URL} -Pjacoco".toString(), currentProjectVersion, '-Sonar')
                                    }
                                }
                            }
                        }
                    }
                }
        )

        if (!isReleasable(currentProjectVersion)) {
            return
        }

        def userAbortedRelease = false
        def releaseVersion
        stage('Continue to Release') {
            try {
                milestone label: 'preReleaseConfirmation'
                timeout(time: 1, unit: 'DAYS') {
                    releaseVersion = input(
                            message: 'Publish ?',
                            parameters: [
                                    [name        : 'version',
                                     defaultValue: toReleaseVersion(currentProjectVersion),
                                     description : 'Release version',
                                     $class      : 'hudson.model.StringParameterDefinition']
                            ]
                    )
                }
                milestone label: 'postReleaseConfirmation'
            } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException e) {
                userAbortedRelease = true
                currentBuild.result = 'SUCCESS'
            }
        }

        if (userAbortedRelease) {
            return
        }

        node {
            gitlabCommitStatus('release') {
                ansiColor('xterm') {
                    stage('Release') {
                        checkout scm
                        sh 'git clean -dfx && git reset --hard'
                        sh "git tag v${releaseVersion}"

                        def descriptor = Artifactory.mavenDescriptor()
                        descriptor.version = releaseVersion
                        descriptor.failOnSnapshot = true
                        descriptor.transform()

                        maven('-DskipTests', releaseVersion, '-Release')

                        sh 'git push --tags'
                    }

                    stage('update version in HEAD') {
                        sh "git checkout ${env.BRANCH_NAME}"
                        sh 'git clean -dfx && git reset --hard'

                        def snapshotVersion = nextSnapshotVersionFor(releaseVersion)

                        def descriptor = Artifactory.mavenDescriptor()
                        descriptor.version = snapshotVersion
                        descriptor.transform()

                        sh "git commit -a -m '[CD] change version to ${snapshotVersion}'"
                        sh 'git push'
                    }
                }
            }
        }
    } catch (err) {
        currentBuild.result = "FAILED"
        echo err.toString()
        throw err
    } finally {
        notifyBuild(currentBuild.result)
    }
}

def isReleasable(version) {
    return !isFeatureBranch() && isSnapshotVersion(version)
}

def isPublishable(version) {
    return !isFeatureBranch() || isFeatureBranchVersion(version)
}

def isFeatureBranch() {
    return env.BRANCH_NAME != 'master'
}

static def isFeatureBranchVersion(version) {
     return version ==~ /\d+\.\d+\.\d+-\p{Upper}+-\d+(:?-SNAPSHOT)?/
}

static def isSnapshotVersion(version) {
    return version.endsWith('-SNAPSHOT')
}

def branchProhibitsSonar() {
    return isFeatureBranch()
}

static def nextSnapshotVersionFor(version) {
    def versions = (version =~ /(\d+\.\d+\.)(\d+)(-.+)?/)
    return "${versions[0][1]}${versions[0][2].toInteger() + 1}" + (versions[0][3] ?: '') + '-SNAPSHOT'
}

static def toReleaseVersion(version) {
    return version.minus('-SNAPSHOT')
}

def maven(goals, version, buildInfoQualifier) {
    def artifactory = Artifactory.server 'artifactory'

    configFileProvider([configFile(fileId: 'simple-maven-settings', variable: 'MAVEN_USER_SETTINGS')]) {
        def mavenRuntime = Artifactory.newMavenBuild()
        mavenRuntime.tool = 'maven350'
        mavenRuntime.resolver server: artifactory, releaseRepo: 'maven-dlabs', snapshotRepo: 'maven-dlabs'
        mavenRuntime.deployer server: artifactory, releaseRepo: 'maven-dlabs-release', snapshotRepo: 'maven-dlabs-snapshot'
        mavenRuntime.deployer.deployArtifacts = isPublishable(version) && buildInfoQualifier != '-Sonar'

        try {
            def buildInfo = mavenRuntime.run pom: 'pom.xml', goals: "-B -s ${MAVEN_USER_SETTINGS} ${goals}".toString()
            if (mavenRuntime.deployer.deployArtifacts) {
                buildInfo.number += buildInfoQualifier
                artifactory.publishBuildInfo buildInfo
            }
        } finally {
            junit allowEmptyResults: true, testResults: '**/target/*-reports/TEST-*.xml'
        }
    }
}

def notifyBuild(String buildStatus) {
    buildStatus = buildStatus ?: 'SUCCESS'

    if (buildStatus == 'STARTED') {
        return
    }

    def subject = "${env.JOB_NAME} [${env.BUILD_NUMBER}]: ${buildStatus}"
    def summary = "${subject}, see the build ${env.BUILD_URL}"

    hipchatSend(color: buildStatus == 'SUCCESS' ? 'GREEN' : 'RED', notify: true, message: summary)

    if (buildStatus == 'FAILED' && !isFeatureBranch()) {
        emailext subject: subject,
                recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
                mimeType: 'text/plain',
                body: "project build error is here: ${env.BUILD_URL}"
    }
}
