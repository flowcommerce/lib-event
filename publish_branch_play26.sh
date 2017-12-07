#!/usr/bin/env bash

# update current version to Play26
BUID_SBT_VERSION=$(cat build.sbt | grep 'version' | grep -o '".*"' | sed 's/"//g')
echo "Current SBT version: BUID_SBT_VERSION"
BRANCH_NAME=$(git branch | awk '/^\*/{print $2}')

NEW_VERSION="$BUID_SBT_VERSION-$BRANCH_NAME"

sed -i '' -e "s/^version.*/version := \"$NEW_VERSION\"/g" build.sbt

REPO_NAME=$(basename -s .git `git config --get remote.origin.url`)

echo "Tagging repository: $REPO_NAME version: $NEW_VERSION"
dev tag $NEW_VERSION

ORG=$(cat build.sbt | grep '^organization' | grep -o '".*"' | sed 's/"//g')
NAME=$(cat build.sbt | grep '^name' | grep -o '".*"' | sed 's/"//g')
ARTIFACT="\"$ORG\" %% \"$NAME\" % \"$NEW_VERSION\""

echo "Publishing artifact: $ARTIFACT"
sbt +publish

