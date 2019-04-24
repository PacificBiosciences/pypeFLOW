package pacbio;

import com.atlassian.bamboo.specs.api.BambooSpec;
import com.atlassian.bamboo.specs.api.builders.BambooKey;
import com.atlassian.bamboo.specs.api.builders.BambooOid;
import com.atlassian.bamboo.specs.api.builders.permission.PermissionType;
import com.atlassian.bamboo.specs.api.builders.permission.Permissions;
import com.atlassian.bamboo.specs.api.builders.permission.PlanPermissions;
import com.atlassian.bamboo.specs.api.builders.plan.Job;
import com.atlassian.bamboo.specs.api.builders.plan.Plan;
import com.atlassian.bamboo.specs.api.builders.plan.PlanIdentifier;
import com.atlassian.bamboo.specs.api.builders.plan.Stage;
import com.atlassian.bamboo.specs.api.builders.plan.branches.BranchCleanup;
import com.atlassian.bamboo.specs.api.builders.plan.branches.PlanBranchManagement;
import com.atlassian.bamboo.specs.api.builders.plan.configuration.ConcurrentBuilds;
import com.atlassian.bamboo.specs.api.builders.project.Project;
import com.atlassian.bamboo.specs.api.builders.requirement.Requirement;
import com.atlassian.bamboo.specs.builders.task.CheckoutItem;
import com.atlassian.bamboo.specs.builders.task.ScriptTask;
import com.atlassian.bamboo.specs.builders.task.VcsCheckoutTask;
import com.atlassian.bamboo.specs.builders.trigger.BitbucketServerTrigger;
import com.atlassian.bamboo.specs.model.task.ScriptTaskProperties;
import com.atlassian.bamboo.specs.util.BambooServer;

@BambooSpec
public class PlanSpec {

    public Plan plan() {
        final Plan plan = new Plan(new Project()

                .key(new BambooKey("SAT"))
                .name("SMRT Analysis Tools (SAT)"),
            "pypeflow3",
            new BambooKey("PYPBS"))
            .description("Plan created from Bamboo Java Specs,  modify http://bitbucket.pacificbiosciences.com:7990/projects/SAT/repos/pypeflow3/browse project to update the plan.")

            .pluginConfigurations(new ConcurrentBuilds()
                    .useSystemWideDefault(false)
                    .maximumNumberOfConcurrentBuilds(4))
            .stages(new Stage("Default Stage")
                    .jobs(new Job("Default Job",
                            new BambooKey("JOB1"))
                            .tasks(new VcsCheckoutTask()
                                    .description("Checkout Default Repository")
                                    .checkoutItems(new CheckoutItem().defaultRepository()),
                                new ScriptTask()
                                    .description("build")
                                    .location(ScriptTaskProperties.Location.FILE)
                                    .fileFromPath("build.sh"))
                            .requirements(new Requirement("system.os")
                                    .matchValue("linux")
                                    .matchType(Requirement.MatchType.EQUALS))))
            .linkedRepositories("pypeflow3")

            .triggers(new BitbucketServerTrigger())
            .planBranchManagement(new PlanBranchManagement()
                .createForPullRequest()
                .delete(new BranchCleanup()
                    .whenRemovedFromRepositoryAfterDays(7)
                    .whenInactiveInRepositoryAfterDays(30))
                .notificationForCommitters())
            .forceStopHungBuilds();
        return plan;
    }

    public PlanPermissions planPermission() {
        final PlanPermissions planPermission = new PlanPermissions(new PlanIdentifier("SAT", "PYPBS"))
            .permissions(new Permissions()
                    .userPermissions("cdunn", PermissionType.VIEW, PermissionType.BUILD, PermissionType.CLONE, PermissionType.EDIT, PermissionType.ADMIN)
                    .userPermissions("bli", PermissionType.BUILD, PermissionType.CLONE, PermissionType.ADMIN, PermissionType.VIEW, PermissionType.EDIT));
        return planPermission;
    }

    public static void main(String... argv) {
        //By default credentials are read from the '.credentials' file.
        BambooServer bambooServer = new BambooServer("http://bamboo.pacificbiosciences.com:8085");
        final PlanSpec planSpec = new PlanSpec();

        final Plan plan = planSpec.plan();
        bambooServer.publish(plan);

        final PlanPermissions planPermission = planSpec.planPermission();
        bambooServer.publish(planPermission);
    }
}
