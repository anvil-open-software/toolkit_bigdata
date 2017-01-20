#!groovy​

def currentPomVersion
node {
    ansiColor('xterm') {
        stage('checkout') {
            checkout scm
            sh "git clean -dfx && git reset --hard"

            currentPomVersion = readMavenPom().version
        }

        stage('build') {
            maven()
        }
    }
}


if (branchProhibitsRelease() || !currentPomVersion.endsWith("-SNAPSHOT")) {
    return
}

node {
    ansiColor('xterm') {
        stage('checkout') {
            checkout scm
            sh "git clean -dfx && git reset --hard"
        }

        stage('SonarQube analysis') {
            withSonarQubeEnv('dlabs') {
                maven("clean install ${env.SONAR_MAVEN_GOAL} -Dsonar.host.url=${env.SONAR_HOST_URL} -Pjacoco".toString())
            }
        }
    }
}

def releaseVersion
stage('Continue to Release') {
    milestone label: 'preReleaseConfirmation'
    timeout(time: 1, unit: 'DAYS') {
        releaseVersion = input(
                message: 'Publish ?',
                parameters: [
                        [name        : 'version',
                         defaultValue: currentPomVersion.minus('-SNAPSHOT'),
                         description : 'Release version',
                         $class      : 'hudson.model.StringParameterDefinition']
                ]
        )
    }
    milestone label: 'postReleaseConfirmation'
}

node {
    stage('checkout Release') {
        checkout scm
        sh "git clean -dfx && git reset --hard"

        maven('-DreleaseVersion=${releaseVersion}" ' +
                '-DsuppressCommitBeforeTag=true -DremoteTagging=false -DupdateWorkingCopyVersions=false')
    }

    stage('update version in HEAD') {
        sh "git checkout ${env.BRANCH_NAME}"
        sh "git clean -dfx && git reset --hard"

        def snapshotVersion = nextSnapshotVersionFor(releaseVersion)
        maven("release:update-versions -DdevelopmentVersion=${snapshotVersion}".toString())
        sh "git commit -a -m '[CD] change version to ${snapshotVersion}'"
        sh "git push"
    }
}

def branchProhibitsRelease() {
    return env.BRANCH_NAME != 'DLABS-901'
}

static nextSnapshotVersionFor(version) {
    def versions = (version =~ /(\d+\.\d+\.)(\d+)/)
    return "${versions[0][1]}${versions[0][2].toInteger() + 1}" + '-SNAPSHOT'
}

// https://wiki.jenkins-ci.org/display/JENKINS/Artifactory+-+Working+With+the+Pipeline+Jenkins+Plugin
def maven(goals) {
    def artifactory = Artifactory.server 'artifactory'

    configFileProvider([configFile(fileId: 'simple-maven-settings', variable: 'MAVEN_USER_SETTINGS')]) {
        def mavenRuntime = Artifactory.newMavenBuild()
        mavenRuntime.resolver server: artifactory, releaseRepo: 'maven-dlabs', snapshotRepo: 'maven-dlabs'
        mavenRuntime.deployer server: artifactory, releaseRepo: 'maven-dlabs-release', snapshotRepo: 'maven-dlabs-snapshot'
        mavenRuntime.tool = 'Maven'

        try {
            def buildInfo = mavenRuntime.run pom: 'pom.xml', goals: "-B -s ${MAVEN_USER_SETTINGS} ${goals}".toString()
            artifactory.publishBuildInfo buildInfo
        } finally {
            junit allowEmptyResults: true, testResults: '**/target/*-reports/TEST-*.xml'
        }
    }
}