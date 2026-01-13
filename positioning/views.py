"""
Views for the Positioning Wizard - ICP and Transformation Analysis.
"""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.views.generic import ListView, DetailView, CreateView, UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
from django.urls import reverse_lazy
from django.contrib import messages
from django.template.loader import render_to_string

from .models import ICP, TransformationAnalysis, LeadMagnetConcept
from .services import TransformationService, ICPSuggestionService, LeadMagnetGeneratorService


# =============================================================================
# DYNAMIC EXAMPLE GENERATION
# =============================================================================

# Industry-specific pain points mapping
INDUSTRY_PAIN_POINTS = {
    'technology': [
        'Difficulty standing out in a crowded market',
        'Long enterprise sales cycles',
        'High customer acquisition costs',
        'Talent retention and recruitment challenges',
        'Keeping up with rapid technology changes',
    ],
    'saas': [
        'High churn rates and customer retention',
        'Long sales cycles with enterprise clients',
        'Difficulty demonstrating ROI to prospects',
        'Competition from well-funded startups',
        'Scaling customer success operations',
    ],
    'healthcare': [
        'Complex regulatory compliance requirements',
        'Long procurement and approval processes',
        'Data security and HIPAA concerns',
        'Difficulty reaching decision makers',
        'Budget constraints and cost justification',
    ],
    'finance': [
        'Strict regulatory and compliance requirements',
        'Building trust with risk-averse clients',
        'Competition from established institutions',
        'Data security and privacy concerns',
        'Slow adoption of new technologies',
    ],
    'insurance': [
        'Complex regulatory compliance landscape',
        'Commoditization and price competition',
        'Legacy systems hindering innovation',
        'Customer retention and policy renewals',
        'Difficulty reaching younger demographics',
    ],
    'ecommerce': [
        'Rising customer acquisition costs',
        'Cart abandonment and conversion issues',
        'Competition from Amazon and big retailers',
        'Inventory management challenges',
        'Building customer loyalty and repeat purchases',
    ],
    'retail': [
        'Competition from online retailers',
        'Inventory and supply chain management',
        'Customer foot traffic decline',
        'Staffing and labor costs',
        'Omnichannel experience expectations',
    ],
    'marketing': [
        'Proving ROI and attribution',
        'Keeping up with algorithm changes',
        'Standing out in a saturated market',
        'Client retention and churn',
        'Scaling service delivery',
    ],
    'consulting': [
        'Feast or famine revenue cycles',
        'Differentiating from competitors',
        'Scaling beyond founder capacity',
        'Long sales cycles with enterprises',
        'Productizing services for scalability',
    ],
    'legal': [
        'Billable hour pressure and utilization',
        'Client acquisition and business development',
        'Technology adoption resistance',
        'Talent retention in competitive market',
        'Fee pressure from corporate clients',
    ],
    'accounting': [
        'Seasonal workload fluctuations',
        'Commoditization of compliance services',
        'Staff retention during busy season',
        'Technology disruption from automation',
        'Client advisory service expansion',
    ],
    'manufacturing': [
        'Supply chain disruptions and costs',
        'Skilled labor shortage',
        'Equipment maintenance and downtime',
        'Quality control consistency',
        'Raw material price volatility',
    ],
    'construction': [
        'Project delays and cost overruns',
        'Skilled labor shortage',
        'Material price volatility',
        'Safety compliance requirements',
        'Cash flow timing on large projects',
    ],
    'hospitality': [
        'Staffing challenges and turnover',
        'Seasonal demand fluctuations',
        'Online review management',
        'Competition from Airbnb and alternatives',
        'Rising operational costs',
    ],
    'restaurant': [
        'Thin profit margins',
        'Staff turnover and training',
        'Food cost management',
        'Online ordering and delivery competition',
        'Customer acquisition in saturated market',
    ],
    'nonprofit': [
        'Donor acquisition and retention',
        'Grant writing and funding uncertainty',
        'Volunteer management and engagement',
        'Demonstrating impact and outcomes',
        'Board engagement and governance',
    ],
    'education': [
        'Budget constraints and funding limitations',
        'Resistance to change from stakeholders',
        'Measuring and proving outcomes',
        'Engaging students in remote/hybrid settings',
        'Keeping curriculum current and relevant',
    ],
    'real estate': [
        'Long transaction cycles',
        'Market volatility and uncertainty',
        'Lead generation and qualification',
        'Building trust with first-time buyers',
        'Standing out in a competitive market',
    ],
    'coaching': [
        'Attracting high-value clients consistently',
        'Scaling beyond 1-on-1 delivery',
        'Differentiating from other coaches',
        'Proving transformation and results',
        'Building a sustainable referral pipeline',
    ],
    'fitness': [
        'Member retention and churn',
        'Seasonal enrollment fluctuations',
        'Competition from boutique studios',
        'Staff training and retention',
        'Equipment maintenance costs',
    ],
    'agency': [
        'Client retention and churn',
        'Scope creep and project profitability',
        'Talent acquisition and retention',
        'Differentiating from competitors',
        'Scaling without sacrificing quality',
    ],
    'media': [
        'Declining traditional revenue streams',
        'Audience fragmentation',
        'Content monetization challenges',
        'Competition for attention',
        'Adapting to platform algorithm changes',
    ],
    'logistics': [
        'Rising fuel and transportation costs',
        'Driver shortage and retention',
        'Last-mile delivery challenges',
        'Technology integration complexity',
        'Customer delivery expectations',
    ],
    'agriculture': [
        'Weather and climate unpredictability',
        'Commodity price volatility',
        'Labor availability and costs',
        'Equipment and technology costs',
        'Regulatory compliance burden',
    ],
    'default': [
        'Struggling to find qualified leads',
        'Long sales cycles',
        'Difficulty reaching decision makers',
        'High customer acquisition costs',
        'Lack of predictable revenue',
    ],
}

# Company size specific pain points
SIZE_PAIN_POINTS = {
    'solo': [
        'Wearing too many hats',
        'Limited time for business development',
    ],
    'small': [
        'Limited budget for marketing',
        'Competing with larger companies',
    ],
    'medium': [
        'Scaling operations efficiently',
        'Maintaining culture during growth',
    ],
    'large': [
        'Organizational silos',
        'Slow decision-making processes',
    ],
    'enterprise': [
        'Legacy system integration',
        'Cross-departmental alignment',
    ],
}

# Industry-specific goals mapping
INDUSTRY_GOALS = {
    'technology': [
        'Increase market share in target segment',
        'Reduce time-to-close for enterprise deals',
        'Build thought leadership in the space',
        'Achieve sustainable growth trajectory',
        'Expand into new markets or verticals',
    ],
    'saas': [
        'Reduce churn and increase retention',
        'Grow MRR/ARR predictably',
        'Improve customer lifetime value',
        'Scale customer acquisition efficiently',
        'Build a world-class product',
    ],
    'healthcare': [
        'Improve patient outcomes',
        'Streamline compliance processes',
        'Reduce operational costs',
        'Expand service offerings',
        'Build trust with healthcare providers',
    ],
    'finance': [
        'Grow assets under management',
        'Expand client base in target segments',
        'Improve client retention rates',
        'Streamline compliance and reporting',
        'Launch new financial products',
    ],
    'insurance': [
        'Increase policy retention rates',
        'Expand into new product lines',
        'Improve claims processing efficiency',
        'Build digital distribution channels',
        'Attract younger customer demographics',
    ],
    'ecommerce': [
        'Increase average order value',
        'Improve conversion rates',
        'Build a loyal customer base',
        'Expand to new channels or markets',
        'Optimize fulfillment and operations',
    ],
    'retail': [
        'Increase same-store sales',
        'Build omnichannel capabilities',
        'Improve inventory turnover',
        'Enhance customer experience',
        'Expand store footprint strategically',
    ],
    'marketing': [
        'Become the go-to agency in niche',
        'Build recurring revenue streams',
        'Scale without proportional headcount',
        'Improve client results and case studies',
        'Expand service offerings strategically',
    ],
    'consulting': [
        'Build productized service offerings',
        'Create predictable revenue streams',
        'Become recognized thought leader',
        'Scale beyond founder dependency',
        'Land larger enterprise clients',
    ],
    'legal': [
        'Increase billable hour realization',
        'Expand practice areas strategically',
        'Build rainmaker pipeline',
        'Improve operational efficiency',
        'Develop alternative fee arrangements',
    ],
    'accounting': [
        'Expand advisory services revenue',
        'Improve staff utilization rates',
        'Build year-round revenue streams',
        'Automate compliance work',
        'Attract and retain top talent',
    ],
    'manufacturing': [
        'Improve operational efficiency',
        'Reduce production costs',
        'Expand product lines',
        'Enter new geographic markets',
        'Achieve quality certifications',
    ],
    'construction': [
        'Improve project profitability',
        'Build strategic partnerships',
        'Expand service capabilities',
        'Strengthen safety record',
        'Develop recurring revenue streams',
    ],
    'hospitality': [
        'Increase occupancy rates',
        'Improve guest satisfaction scores',
        'Build direct booking channels',
        'Expand loyalty program',
        'Optimize revenue per available room',
    ],
    'restaurant': [
        'Increase average check size',
        'Build online ordering revenue',
        'Improve table turnover',
        'Expand catering business',
        'Build brand recognition locally',
    ],
    'nonprofit': [
        'Diversify funding sources',
        'Increase donor retention rates',
        'Expand program reach and impact',
        'Build sustainable revenue model',
        'Strengthen board governance',
    ],
    'education': [
        'Improve student outcomes and success',
        'Increase enrollment and retention',
        'Expand program offerings',
        'Build industry partnerships',
        'Achieve accreditation or recognition',
    ],
    'real estate': [
        'Increase transaction volume',
        'Build a consistent referral network',
        'Expand into new markets',
        'Develop passive income streams',
        'Build a recognizable personal brand',
    ],
    'coaching': [
        'Fill programs consistently',
        'Increase revenue per client',
        'Build scalable group programs',
        'Create passive income streams',
        'Become the go-to expert in niche',
    ],
    'fitness': [
        'Increase member retention',
        'Grow recurring membership revenue',
        'Expand class offerings',
        'Build personal training revenue',
        'Develop corporate wellness partnerships',
    ],
    'agency': [
        'Increase retainer-based revenue',
        'Improve project profitability',
        'Build specialized expertise',
        'Expand geographic reach',
        'Develop strategic partnerships',
    ],
    'media': [
        'Grow subscription revenue',
        'Diversify revenue streams',
        'Build engaged audience community',
        'Expand content formats',
        'Develop brand partnerships',
    ],
    'logistics': [
        'Improve delivery efficiency',
        'Reduce operational costs',
        'Expand service territory',
        'Build technology capabilities',
        'Develop strategic partnerships',
    ],
    'agriculture': [
        'Increase yield per acre',
        'Diversify crop or product mix',
        'Reduce input costs',
        'Build direct-to-consumer channels',
        'Achieve sustainability certifications',
    ],
    'default': [
        'Increase revenue and profitability',
        'Build a predictable sales pipeline',
        'Improve customer satisfaction',
        'Scale operations efficiently',
        'Become a market leader',
    ],
}

# Company size specific goals
SIZE_GOALS = {
    'solo': [
        'Build consistent income stream',
        'Free up time for high-value work',
    ],
    'small': [
        'Hire first key team members',
        'Achieve profitability milestone',
    ],
    'medium': [
        'Professionalize operations',
        'Expand market presence',
    ],
    'large': [
        'Drive innovation initiatives',
        'Strengthen market position',
    ],
    'enterprise': [
        'Transform digital capabilities',
        'Lead industry innovation',
    ],
}

# =============================================================================
# B2C PAIN POINTS AND GOALS
# =============================================================================

# B2C niche-specific pain points
B2C_NICHE_PAIN_POINTS = {
    'fitness': [
        'Struggling to stay motivated and consistent',
        'Not seeing results despite effort',
        'Overwhelmed by conflicting fitness advice',
        'Difficulty fitting workouts into busy schedule',
        'Gym intimidation or social anxiety',
    ],
    'health': [
        'Confusion about what foods are actually healthy',
        'Difficulty breaking unhealthy habits',
        'Feeling tired and low energy throughout the day',
        'Struggling to manage stress and anxiety',
        'Healthcare costs and insurance complexity',
    ],
    'personal finance': [
        'Living paycheck to paycheck',
        'Overwhelmed by debt and financial obligations',
        'No idea how to start investing',
        'Difficulty saving for future goals',
        'Fear of never being able to retire',
    ],
    'career': [
        'Feeling stuck in current job',
        'Lack of clear career direction',
        'Difficulty getting promoted or raises',
        'Work-life balance struggles',
        'Imposter syndrome and self-doubt',
    ],
    'relationships': [
        'Difficulty meeting quality partners',
        'Communication issues in relationships',
        'Loneliness and social isolation',
        'Trust issues from past experiences',
        'Balancing personal needs with partner needs',
    ],
    'parenting': [
        'Constant exhaustion and burnout',
        'Worry about making the right decisions',
        'Screen time and technology challenges',
        'Balancing work and family demands',
        'Feeling isolated from adult interaction',
    ],
    'home improvement': [
        'Projects taking longer than expected',
        'Going over budget on renovations',
        'Not knowing which contractors to trust',
        'DIY projects turning out poorly',
        'Difficulty prioritizing what to fix first',
    ],
    'beauty': [
        'Skin problems that won\'t go away',
        'Overwhelmed by product options',
        'Wasting money on products that don\'t work',
        'Aging concerns and self-image issues',
        'Sensitivity and allergic reactions',
    ],
    'cooking': [
        'No time to prepare healthy meals',
        'Boredom with same recipes',
        'Food waste from unused ingredients',
        'Picky eaters in the family',
        'Difficulty eating healthy on a budget',
    ],
    'travel': [
        'Overwhelmed by trip planning',
        'Fear of overpaying for travel',
        'Safety concerns in new places',
        'FOMO and decision paralysis',
        'Difficulty taking time off work',
    ],
    'education': [
        'Struggling to learn new skills',
        'Lack of time for self-improvement',
        'Information overload and analysis paralysis',
        'Difficulty staying focused when studying',
        'Not knowing what skills to develop',
    ],
    'pets': [
        'Behavioral issues with pets',
        'High costs of pet care and vet bills',
        'Finding reliable pet care when traveling',
        'Health concerns for aging pets',
        'Guilt about not spending enough time with pets',
    ],
    'default': [
        'Feeling overwhelmed by options and information',
        'Struggling to find time for what matters',
        'Difficulty achieving desired results',
        'Worry about making wrong decisions',
        'Frustration with current situation',
    ],
}

# B2C age-specific pain points
AGE_PAIN_POINTS = {
    '18-24': [
        'Financial stress from student debt',
        'Uncertainty about career path',
    ],
    '25-34': [
        'Pressure to hit life milestones',
        'Balancing career growth and personal life',
    ],
    '35-44': [
        'Sandwich generation pressures',
        'Mid-career stagnation fears',
    ],
    '45-54': [
        'Concerns about retirement readiness',
        'Health changes and energy decline',
    ],
    '55-64': [
        'Planning for retirement transition',
        'Empty nest adjustment',
    ],
    '65+': [
        'Maintaining independence and health',
        'Fixed income budgeting',
    ],
}

# B2C niche-specific goals
B2C_NICHE_GOALS = {
    'fitness': [
        'Lose weight and keep it off',
        'Build strength and muscle tone',
        'Increase energy and stamina',
        'Develop a sustainable fitness routine',
        'Improve athletic performance',
    ],
    'health': [
        'Feel more energetic throughout the day',
        'Develop healthy eating habits',
        'Reduce stress and improve mental health',
        'Sleep better and wake up refreshed',
        'Prevent chronic health conditions',
    ],
    'personal finance': [
        'Build an emergency fund',
        'Pay off debt and become debt-free',
        'Start investing for the future',
        'Save for a major purchase (home, car)',
        'Achieve financial independence',
    ],
    'career': [
        'Get promoted or advance in career',
        'Find more fulfilling work',
        'Increase income significantly',
        'Start a side business or freelance',
        'Achieve better work-life balance',
    ],
    'relationships': [
        'Find a compatible long-term partner',
        'Improve communication with partner',
        'Build deeper, more meaningful connections',
        'Heal from past relationship trauma',
        'Create a supportive social circle',
    ],
    'parenting': [
        'Raise confident, well-adjusted children',
        'Create quality family time',
        'Balance parenting with self-care',
        'Prepare children for future success',
        'Build stronger family relationships',
    ],
    'home improvement': [
        'Create a dream living space',
        'Increase home value',
        'Make home more energy efficient',
        'Complete DIY projects successfully',
        'Organize and declutter living space',
    ],
    'beauty': [
        'Achieve clear, healthy skin',
        'Find a routine that actually works',
        'Look younger and more vibrant',
        'Build confidence in appearance',
        'Develop a sustainable self-care routine',
    ],
    'cooking': [
        'Cook healthy meals in less time',
        'Master new cuisines and techniques',
        'Meal prep efficiently for the week',
        'Impress guests with cooking skills',
        'Save money by cooking at home',
    ],
    'travel': [
        'Visit dream destinations',
        'Travel more often on a budget',
        'Have authentic local experiences',
        'Create lasting memories with loved ones',
        'Explore new cultures confidently',
    ],
    'education': [
        'Learn a valuable new skill',
        'Earn a degree or certification',
        'Stay competitive in job market',
        'Expand knowledge in area of interest',
        'Develop expertise in a field',
    ],
    'pets': [
        'Have a well-behaved, happy pet',
        'Provide the best care for pets',
        'Build a strong bond with pets',
        'Keep pets healthy and active',
        'Find reliable pet care solutions',
    ],
    'default': [
        'Improve quality of life',
        'Achieve personal goals faster',
        'Feel more confident and in control',
        'Save time and reduce stress',
        'Build a better future',
    ],
}

# B2C age-specific goals
AGE_GOALS = {
    '18-24': [
        'Build a solid financial foundation',
        'Launch a successful career',
    ],
    '25-34': [
        'Establish long-term stability',
        'Build wealth and assets',
    ],
    '35-44': [
        'Maximize earning potential',
        'Set up children for success',
    ],
    '45-54': [
        'Secure retirement readiness',
        'Enjoy peak life experiences',
    ],
    '55-64': [
        'Plan smooth transition to retirement',
        'Focus on health and wellbeing',
    ],
    '65+': [
        'Enjoy an active, fulfilling retirement',
        'Leave a meaningful legacy',
    ],
}


def get_example_pain_points(industry: str, company_size: str, customer_type: str = 'b2b', age_range: str = '') -> list:
    """Generate relevant example pain points based on customer type, industry/niche, and demographics."""
    industry_lower = industry.lower().strip() if industry else ''

    if customer_type == 'b2c':
        # B2C: Use niche-based pain points
        niche_points = None
        for key in B2C_NICHE_PAIN_POINTS:
            if key in industry_lower or industry_lower in key:
                niche_points = B2C_NICHE_PAIN_POINTS[key]
                break

        if not niche_points:
            niche_points = B2C_NICHE_PAIN_POINTS['default']

        # Get age-specific points
        age_points = AGE_PAIN_POINTS.get(age_range, [])

        # Combine: 3 niche-specific + 2 age-specific
        return niche_points[:3] + age_points[:2]
    else:
        # B2B: Use industry-based pain points
        industry_points = None
        for key in INDUSTRY_PAIN_POINTS:
            if key in industry_lower or industry_lower in key:
                industry_points = INDUSTRY_PAIN_POINTS[key]
                break

        if not industry_points:
            industry_points = INDUSTRY_PAIN_POINTS['default']

        # Get size-specific points
        size_points = SIZE_PAIN_POINTS.get(company_size, [])

        # Combine: 3 industry-specific + 2 size-specific
        return industry_points[:3] + size_points[:2]


def get_example_goals(industry: str, company_size: str, customer_type: str = 'b2b', age_range: str = '') -> list:
    """Generate relevant example goals based on customer type, industry/niche, and demographics."""
    industry_lower = industry.lower().strip() if industry else ''

    if customer_type == 'b2c':
        # B2C: Use niche-based goals
        niche_goals = None
        for key in B2C_NICHE_GOALS:
            if key in industry_lower or industry_lower in key:
                niche_goals = B2C_NICHE_GOALS[key]
                break

        if not niche_goals:
            niche_goals = B2C_NICHE_GOALS['default']

        # Get age-specific goals
        age_goals = AGE_GOALS.get(age_range, [])

        # Combine: 3 niche-specific + 2 age-specific
        return niche_goals[:3] + age_goals[:2]
    else:
        # B2B: Use industry-based goals
        industry_goals = None
        for key in INDUSTRY_GOALS:
            if key in industry_lower or industry_lower in key:
                industry_goals = INDUSTRY_GOALS[key]
                break

        if not industry_goals:
            industry_goals = INDUSTRY_GOALS['default']

        # Get size-specific goals
        size_goals = SIZE_GOALS.get(company_size, [])

        # Combine: 3 industry-specific + 2 size-specific
        return industry_goals[:3] + size_goals[:2]


class ICPListView(LoginRequiredMixin, ListView):
    """List all ICPs for the current user."""

    model = ICP
    template_name = 'positioning/icp_list.html'
    context_object_name = 'icps'

    def get_queryset(self):
        return ICP.objects.filter(user=self.request.user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transformations'] = TransformationAnalysis.objects.filter(
            user=self.request.user
        )[:5]
        return context


class ICPCreateView(LoginRequiredMixin, View):
    """Multi-step wizard for creating an ICP."""

    template_name = 'positioning/icp_form.html'

    def _get_context(self, step, wizard_data):
        """Build context with dynamic examples based on wizard data."""
        industry = wizard_data.get('industry', '')
        company_size = wizard_data.get('company_size', '')
        customer_type = wizard_data.get('customer_type', 'b2b')
        age_range = wizard_data.get('age_range', '')

        return {
            'step': step,  # Keep as string for template concatenation
            'wizard_data': wizard_data,
            'company_size_choices': ICP.COMPANY_SIZE_CHOICES,
            'age_range_choices': ICP.AGE_RANGE_CHOICES,
            'income_level_choices': ICP.INCOME_LEVEL_CHOICES,
            'example_pain_points': get_example_pain_points(industry, company_size, customer_type, age_range),
            'example_goals': get_example_goals(industry, company_size, customer_type, age_range),
        }

    def get(self, request):
        """Display the ICP creation wizard."""
        step = request.GET.get('step', '1')

        # Get session data for the wizard
        wizard_data = request.session.get('icp_wizard', {})
        context = self._get_context(step, wizard_data)

        # For HTMX requests, return only the step partial
        if request.htmx:
            return render(request, f'positioning/partials/icp_step_{step}.html', context)

        return render(request, self.template_name, context)

    def post(self, request):
        """Process wizard step submission."""
        step = request.POST.get('step', '1')
        action = request.POST.get('action', 'next')

        # Initialize or get session data
        wizard_data = request.session.get('icp_wizard', {})

        # Process based on current step
        if step == '1':
            wizard_data['name'] = request.POST.get('name', '')
            wizard_data['customer_type'] = request.POST.get('customer_type', 'b2b')
            wizard_data['industry'] = request.POST.get('industry', '')
            # B2B fields
            wizard_data['company_size'] = request.POST.get('company_size', '')
            # B2C fields
            wizard_data['age_range'] = request.POST.get('age_range', '')
            wizard_data['income_level'] = request.POST.get('income_level', '')
            wizard_data['demographics'] = request.POST.get('demographics', '')

        elif step == '2':
            pain_points = request.POST.getlist('pain_points[]')
            wizard_data['pain_points'] = [p for p in pain_points if p.strip()]

        elif step == '3':
            goals = request.POST.getlist('goals[]')
            wizard_data['goals'] = [g for g in goals if g.strip()]

        elif step == '4':
            wizard_data['budget_range'] = request.POST.get('budget_range', '')

            # Process decision makers as JSON
            decision_makers_raw = request.POST.get('decision_makers', '[]')
            try:
                wizard_data['decision_makers'] = json.loads(decision_makers_raw)
            except json.JSONDecodeError:
                wizard_data['decision_makers'] = []

        # Save to session
        request.session['icp_wizard'] = wizard_data
        request.session.modified = True

        # Handle navigation
        current_step = int(step)

        if action == 'back':
            next_step = max(1, current_step - 1)
        elif action == 'save':
            # Final save - create the ICP
            return self._create_icp(request, wizard_data)
        else:
            next_step = min(4, current_step + 1)

        context = self._get_context(next_step, wizard_data)

        # For HTMX requests, return only the step partial
        if request.htmx:
            return render(request, f'positioning/partials/icp_step_{next_step}.html', context)

        return render(request, self.template_name, context)

    def _create_icp(self, request, wizard_data):
        """Create the ICP from wizard data."""
        try:
            # Check if this should be the primary ICP
            is_primary = not ICP.objects.filter(user=request.user).exists()
            customer_type = wizard_data.get('customer_type', 'b2b')

            icp = ICP.objects.create(
                user=request.user,
                name=wizard_data.get('name', ''),
                customer_type=customer_type,
                industry=wizard_data.get('industry', ''),
                # B2B fields
                company_size=wizard_data.get('company_size', '') if customer_type == 'b2b' else None,
                # B2C fields
                age_range=wizard_data.get('age_range', '') if customer_type == 'b2c' else None,
                income_level=wizard_data.get('income_level', '') if customer_type == 'b2c' else None,
                demographics=wizard_data.get('demographics', '') if customer_type == 'b2c' else None,
                # Common fields
                pain_points=wizard_data.get('pain_points', []),
                goals=wizard_data.get('goals', []),
                budget_range=wizard_data.get('budget_range', ''),
                decision_makers=wizard_data.get('decision_makers', []),
                is_primary=is_primary,
            )

            # Clear wizard session data
            if 'icp_wizard' in request.session:
                del request.session['icp_wizard']

            # Mark onboarding as completed if this is the user's first ICP
            if is_primary and not request.user.onboarding_completed:
                request.user.onboarding_completed = True
                request.user.save()
                messages.success(request, f'ICP "{icp.name}" created successfully! Your setup is complete.')
            else:
                messages.success(request, f'ICP "{icp.name}" created successfully!')

            if request.htmx:
                response = HttpResponse()
                response['HX-Redirect'] = reverse_lazy('positioning:icp_detail', kwargs={'pk': icp.pk})
                return response

            return redirect('positioning:icp_detail', pk=icp.pk)

        except Exception as e:
            messages.error(request, f'Error creating ICP: {str(e)}')
            return redirect('positioning:icp_create')


class ICPDetailView(LoginRequiredMixin, DetailView):
    """View ICP details."""

    model = ICP
    template_name = 'positioning/icp_detail.html'
    context_object_name = 'icp'

    def get_queryset(self):
        return ICP.objects.filter(user=self.request.user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transformations'] = TransformationAnalysis.objects.filter(
            icp=self.object,
            user=self.request.user
        )
        return context


class ICPUpdateView(LoginRequiredMixin, UpdateView):
    """Edit an existing ICP."""

    model = ICP
    template_name = 'positioning/icp_update.html'
    fields = ['name', 'industry', 'company_size', 'pain_points', 'goals',
              'budget_range', 'decision_makers', 'is_primary']

    def get_queryset(self):
        return ICP.objects.filter(user=self.request.user)

    def get_success_url(self):
        return reverse_lazy('positioning:icp_detail', kwargs={'pk': self.object.pk})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['company_size_choices'] = ICP.COMPANY_SIZE_CHOICES
        return context

    def form_valid(self, form):
        response = super().form_valid(form)
        # If this is the primary ICP, mark it as reviewed
        if form.instance.is_primary:
            self.request.user.mark_icp_reviewed()
        messages.success(self.request, f'ICP "{form.instance.name}" updated successfully!')
        return response


class ICPDeleteView(LoginRequiredMixin, View):
    """Delete an ICP."""

    def post(self, request, pk):
        icp = get_object_or_404(ICP, pk=pk, user=request.user)
        name = icp.name
        icp.delete()

        messages.success(request, f'ICP "{name}" deleted successfully!')

        if request.htmx:
            response = HttpResponse()
            response['HX-Redirect'] = reverse_lazy('positioning:icp_list')
            return response

        return redirect('positioning:icp_list')


class ICPSetPrimaryView(LoginRequiredMixin, View):
    """Set an ICP as the primary ICP."""

    def post(self, request, pk):
        icp = get_object_or_404(ICP, pk=pk, user=request.user)

        # Remove primary from all other ICPs
        ICP.objects.filter(user=request.user).update(is_primary=False)

        # Set this one as primary
        icp.is_primary = True
        icp.save()

        messages.success(request, f'"{icp.name}" is now your primary ICP!')

        if request.htmx:
            return render(request, 'positioning/partials/icp_card.html', {'icp': icp})

        return redirect('positioning:icp_detail', pk=pk)


class TransformationCreateView(LoginRequiredMixin, View):
    """Create a new transformation analysis."""

    template_name = 'positioning/transformation_form.html'

    def get(self, request):
        """Display the transformation creation form."""
        icps = ICP.objects.filter(user=request.user)
        preselected_icp = request.GET.get('icp')

        context = {
            'icps': icps,
            'preselected_icp': preselected_icp,
        }

        return render(request, self.template_name, context)

    def post(self, request):
        """Process transformation creation."""
        icp_id = request.POST.get('icp')
        before_state = request.POST.get('before_state', '')
        after_state = request.POST.get('after_state', '')

        icp = None
        if icp_id:
            icp = get_object_or_404(ICP, pk=icp_id, user=request.user)

        # Create the basic transformation
        transformation = TransformationAnalysis.objects.create(
            user=request.user,
            icp=icp,
            before_state=before_state,
            after_state=after_state,
            transformation_summary='',
            key_obstacles=[],
            value_drivers=[],
            ai_generated=False,
        )

        messages.success(request, 'Transformation analysis created!')
        return redirect('positioning:transformation_detail', pk=transformation.pk)


class TransformationGenerateView(LoginRequiredMixin, View):
    """Generate AI analysis for a transformation."""

    def post(self, request):
        """Generate transformation analysis using AI."""
        icp_id = request.POST.get('icp')
        before_state = request.POST.get('before_state', '')
        after_state = request.POST.get('after_state', '')

        icp = None
        if icp_id:
            icp = get_object_or_404(ICP, pk=icp_id, user=request.user)

        # Use the TransformationService
        service = TransformationService()

        try:
            analysis = service.analyze(
                icp=icp,
                before_state=before_state,
                after_state=after_state,
            )

            # Create the transformation with AI analysis
            transformation = TransformationAnalysis.objects.create(
                user=request.user,
                icp=icp,
                before_state=before_state,
                after_state=after_state,
                transformation_summary=analysis.get('transformation_summary', ''),
                key_obstacles=analysis.get('key_obstacles', []),
                value_drivers=analysis.get('value_drivers', []),
                ai_generated=True,
            )

            if request.htmx:
                # Return the analysis results partial
                return render(request, 'positioning/partials/transformation_results.html', {
                    'transformation': transformation,
                    'analysis': analysis,
                })

            return redirect('positioning:transformation_detail', pk=transformation.pk)

        except Exception as e:
            error_message = f'Error generating analysis: {str(e)}'

            if request.htmx:
                return render(request, 'positioning/partials/transformation_error.html', {
                    'error': error_message,
                })

            messages.error(request, error_message)
            return redirect('positioning:transformation_create')


class TransformationDetailView(LoginRequiredMixin, DetailView):
    """View transformation analysis details."""

    model = TransformationAnalysis
    template_name = 'positioning/transformation_detail.html'
    context_object_name = 'transformation'

    def get_queryset(self):
        return TransformationAnalysis.objects.filter(user=self.request.user)


class TransformationListView(LoginRequiredMixin, ListView):
    """List all transformations for the current user."""

    model = TransformationAnalysis
    template_name = 'positioning/transformation_list.html'
    context_object_name = 'transformations'

    def get_queryset(self):
        return TransformationAnalysis.objects.filter(user=self.request.user)


class TransformationDeleteView(LoginRequiredMixin, View):
    """Delete a transformation analysis."""

    def post(self, request, pk):
        transformation = get_object_or_404(
            TransformationAnalysis,
            pk=pk,
            user=request.user
        )
        transformation.delete()

        messages.success(request, 'Transformation analysis deleted!')

        if request.htmx:
            response = HttpResponse()
            response['HX-Redirect'] = reverse_lazy('positioning:transformation_list')
            return response

        return redirect('positioning:transformation_list')


class GenerateTransformationDraftView(LoginRequiredMixin, View):
    """Generate a draft before/after transformation based on ICP."""

    def post(self, request):
        """Generate transformation draft using ICP data."""
        try:
            data = json.loads(request.body)
            icp_id = data.get('icp_id')
        except json.JSONDecodeError:
            icp_id = request.POST.get('icp_id')

        if not icp_id:
            return JsonResponse({'error': 'ICP ID required'}, status=400)

        icp = get_object_or_404(ICP, pk=icp_id, user=request.user)

        # Generate before state from pain points
        pain_points = icp.pain_points or []
        before_parts = []
        if pain_points:
            before_parts.append(f"Customers are experiencing: {pain_points[0]}")
            if len(pain_points) > 1:
                before_parts.append(f"They also struggle with {pain_points[1].lower()}")
            if len(pain_points) > 2:
                before_parts.append(f"and {pain_points[2].lower()}")

        before_state = ". ".join(before_parts) + "." if before_parts else ""

        # Generate after state from goals
        goals = icp.goals or []
        after_parts = []
        if goals:
            after_parts.append(f"After working with us, customers {goals[0].lower()}")
            if len(goals) > 1:
                after_parts.append(f"They also {goals[1].lower()}")

        after_state = ". ".join(after_parts) + "." if after_parts else ""

        # Fallback if no data
        if not before_state:
            before_state = f"Customers in the {icp.industry} space face challenges with efficiency and growth."
        if not after_state:
            after_state = f"After using our solution, customers achieve their goals and see measurable improvements."

        return JsonResponse({
            'before_state': before_state,
            'after_state': after_state,
        })


class GenerateAISuggestionsView(LoginRequiredMixin, View):
    """Generate AI-powered suggestions for pain points, goals, or decision makers."""

    def post(self, request):
        """Generate AI suggestions based on industry/niche and demographics."""
        suggestion_type = request.POST.get('type', 'pain_points')  # pain_points, goals, or decision_makers
        industry = request.POST.get('industry', '')
        company_size = request.POST.get('company_size', '')
        customer_type = request.POST.get('customer_type', 'b2b')
        age_range = request.POST.get('age_range', '')

        if not industry:
            label = 'niche' if customer_type == 'b2c' else 'industry'
            return HttpResponse(
                f'<div class="text-sm text-red-600">Please enter a {label} first.</div>',
                content_type='text/html'
            )

        # Generate suggestions using AI
        service = ICPSuggestionService()

        if suggestion_type == 'goals':
            suggestions = service.generate_goals(industry, company_size, customer_type, age_range)
            onclick_fn = 'addExample'  # Matches x-data function in icp_step_3.html
        elif suggestion_type == 'decision_makers':
            # Decision makers returns list of objects, not strings
            suggestions = service.generate_decision_makers(industry, company_size, customer_type, age_range)
            return self._render_decision_maker_suggestions(suggestions, industry, customer_type)
        else:
            suggestions = service.generate_pain_points(industry, company_size, customer_type, age_range)
            onclick_fn = 'addExample'  # Matches x-data function in icp_step_2.html

        if not suggestions:
            return HttpResponse(
                '<div class="text-sm text-amber-600">Could not generate suggestions. Using AI requires an API key. Check your configuration.</div>',
                content_type='text/html'
            )

        # Build HTML for the suggestions - matching template button styling
        html_items = []
        for suggestion in suggestions:
            escaped = suggestion.replace("'", "\\'").replace('"', '&quot;')
            html_items.append(
                f'<button type="button" @click="{onclick_fn}(\'{escaped}\')" '
                f'class="block w-full text-left px-3 py-2 text-sm text-apple-gray-500 hover:text-apple-blue hover:bg-white rounded-lg transition-colors">'
                f'+ {suggestion}</button>'
            )

        label = industry
        html = f'''
        <div class="space-y-1">
            {''.join(html_items)}
        </div>
        <p class="mt-2 text-xs text-green-600">AI-generated suggestions for {label}</p>
        '''

        return HttpResponse(html, content_type='text/html')

    def _render_decision_maker_suggestions(self, suggestions, industry, customer_type):
        """Render decision maker suggestions as clickable cards."""
        if not suggestions:
            return HttpResponse(
                '<div class="text-sm text-amber-600">Could not generate suggestions. Using AI requires an API key. Check your configuration.</div>',
                content_type='text/html'
            )

        label = 'Purchase Influencers' if customer_type == 'b2c' else 'Decision Makers'
        role_labels = {
            'decision_maker': 'Final Decision Maker',
            'influencer': 'Influencer',
            'champion': 'Champion',
            'user': 'End User'
        }

        html_items = []
        for i, dm in enumerate(suggestions):
            title = dm.get('title', '').replace("'", "\\'").replace('"', '&quot;')
            role = dm.get('role', 'decision_maker')
            concerns = dm.get('concerns', '').replace("'", "\\'").replace('"', '&quot;')
            role_label = role_labels.get(role, role)

            # JSON encode the decision maker - escape for HTML attribute
            dm_json = json.dumps(dm).replace('"', '&quot;')

            html_items.append(f'''
                <button type="button"
                        id="dm-suggestion-{i}"
                        data-dm="{dm_json}"
                        onclick="selectDecisionMaker(this)"
                        class="dm-suggestion block w-full text-left p-3 bg-white border border-apple-gray-200 rounded-lg hover:border-apple-blue hover:bg-apple-blue/5 transition-colors">
                    <div class="flex justify-between items-start">
                        <span class="font-medium text-apple-gray-600">{title}</span>
                        <span class="text-xs px-2 py-0.5 bg-apple-gray-100 text-apple-gray-500 rounded">{role_label}</span>
                    </div>
                    <p class="text-xs text-apple-gray-400 mt-1">{concerns}</p>
                </button>
            ''')

        html = f'''
        <div class="space-y-2">
            {''.join(html_items)}
        </div>
        <p class="mt-2 text-xs text-green-600">AI-generated {label.lower()} for {industry}</p>
        <p id="dm-added-msg" class="hidden mt-2 text-xs text-apple-blue font-medium">Added to form above</p>
        <script>
        function selectDecisionMaker(btn) {{
            // Get the data
            const dm = JSON.parse(btn.dataset.dm);

            // Find the Alpine component on the form
            const form = document.querySelector('form[x-data]');
            if (form && form._x_dataStack) {{
                const alpineData = form._x_dataStack[0];
                if (alpineData && alpineData.addDecisionMakerSuggestion) {{
                    alpineData.addDecisionMakerSuggestion(dm);
                }}
            }}

            // Visual feedback - mark as selected
            btn.classList.remove('bg-white', 'border-apple-gray-200');
            btn.classList.add('bg-green-50', 'border-green-500');
            btn.innerHTML = '<div class="flex items-center justify-center text-green-600"><svg class="w-5 h-5 mr-2" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"></path></svg>Added</div>';
            btn.disabled = true;

            // Show added message
            document.getElementById('dm-added-msg').classList.remove('hidden');

            // Scroll to show the form
            document.querySelector('.space-y-3').scrollIntoView({{ behavior: 'smooth', block: 'start' }});
        }}
        </script>
        '''

        return HttpResponse(html, content_type='text/html')


# =============================================================================
# LEAD MAGNET GENERATOR VIEWS
# =============================================================================

class LeadMagnetListView(LoginRequiredMixin, ListView):
    """List user's lead magnet concepts."""
    model = LeadMagnetConcept
    template_name = 'positioning/lead_magnet_list.html'
    context_object_name = 'lead_magnets'

    def get_queryset(self):
        return LeadMagnetConcept.objects.filter(user=self.request.user)


class LeadMagnetGenerateView(LoginRequiredMixin, View):
    """Generate new lead magnet concepts."""
    template_name = 'positioning/lead_magnet_generate.html'

    def get(self, request):
        """Show form to select transformation."""
        transformations = TransformationAnalysis.objects.filter(user=request.user)
        return render(request, self.template_name, {'transformations': transformations})

    def post(self, request):
        """Generate 3 concepts using LeadMagnetGeneratorService."""
        transformation_id = request.POST.get('transformation')

        if not transformation_id:
            messages.error(request, 'Please select a transformation analysis.')
            return redirect('positioning:lead_magnet_generate')

        transformation = get_object_or_404(
            TransformationAnalysis,
            pk=transformation_id,
            user=request.user
        )

        # Generate concepts using service
        service = LeadMagnetGeneratorService()

        try:
            # Generate unsaved concept objects
            concepts = service.generate_concepts(
                user=request.user,
                transformation=transformation,
                icp=transformation.icp,
                num_concepts=3
            )

            # Save concepts to database
            created_concepts = []
            for concept in concepts:
                concept.save()
                created_concepts.append(concept)

            messages.success(
                request,
                f'Generated {len(created_concepts)} lead magnet concepts!'
            )
            return redirect('positioning:lead_magnet_list')

        except Exception as e:
            messages.error(request, f'Error generating concepts: {str(e)}')
            return redirect('positioning:lead_magnet_generate')


class LeadMagnetDetailView(LoginRequiredMixin, DetailView):
    """View a single lead magnet concept."""
    model = LeadMagnetConcept
    template_name = 'positioning/lead_magnet_detail.html'
    context_object_name = 'lead_magnet'

    def get_queryset(self):
        return LeadMagnetConcept.objects.filter(user=self.request.user)
