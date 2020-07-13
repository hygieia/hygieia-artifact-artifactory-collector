package com.capitalone.dashboard.model;

import java.util.List;

public class RepoAndPattern {
     String repo;
     List<String> subRepos;
     List<String> patterns;

    public String getRepo() {
        return repo;
    }

    public void setRepo(String repo) {
        this.repo = repo;
    }

    public List<String> getSubRepos() {
        return subRepos;
    }

    public void setSubRepos(List<String> subRepos) {
        this.subRepos = subRepos;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    public void setPatterns(List<String> patterns) {
        this.patterns = patterns;
    }


}
