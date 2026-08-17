const fs = require('fs');
const css = fs.readFileSync('tmp-landing-scoped.css', 'utf8');
const head = `<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource-variable/mona-sans@5.0.0/index.min.css">
<style id="dc-landing-styles">
${css}
</style>
`;
fs.writeFileSync('tmp-landing-head.html', head);
console.log('head', head.length);

const existingFooter = `<!-- Memberstack webflow package -->
<!--<script data-memberstack-app="app_cm9yhctsc00vn0wuz3ytk2jkt" src="https://static.memberstack.com/scripts/v1/memberstack.js" type="text/javascript"></script>-->

<script>
window.$memberstackDom.getCurrentMember().then((member) => {
  if (member.data) {
  
  	//$('.quick-sign-in').text('Go to Dashboard').attr('href', member.data.loginRedirect);

	// Replace 'pln_basic-plan-id' with your actual Basic Plan ID
    const basicPlanId = 'pln_basic-47rv0rwf'; 
    const hasBasicPlan = member.data.planConnections.some(
      (plan) => plan.planId === basicPlanId
    );

    if (hasBasicPlan) {
      console.log('User not verified');
      window.$memberstackDom.logout();
    } else {
      $('.quick-sign-in').text('Go to Dashboard').attr('href', member.data.loginRedirect);
    }
    
    //console.log('There is a member logged in:', member);
    // Access member data here, e.g., member.data.id, member.data.email
  } else {
    //console.log('No member is logged in:', member);
    // Handle the case where no member is logged in
  }
});
</script>`;

const landingJs = fs.readFileSync('tmp-landing-scripts.js', 'utf8');
const footer = `${existingFooter}

<script>
${landingJs}
</script>
`;
fs.writeFileSync('tmp-landing-footer.html', footer);
console.log('footer', footer.length);
