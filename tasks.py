"""Tasks for the coaching app.
"""
import chargebee
import datetime
import analytics
import requests

from delighted import Delighted
from celery import shared_task
from celery.task import periodic_task
from celery.schedules import crontab

from django.db.models import Q, Max
from django.conf import settings

from hbs import date_utils, messaging_utils

from accounts.models import UserExternalService, UserProfile
from coaching.models import (NextActions, UserSupporter, CoachStudent,
                             CoachOrganization, CoachLoad, CoachReport,
                             StudentRecord, CoachStudentProgramHistory, StudentGroup)
from scheduling.models import StudyDay
from abilities.models import ReportCard, UserAbility
from messaging.models import UserMessage
from meetings.models import Meeting


@periodic_task(run_every=crontab(day_of_week='monday', hour=14, minute=0))
def update_parents_of_free_students():
    """Send email to parents of free kids after they have completed 3 weeks of the program
    """
    # logic week 1 get all kids who's created on date was within
    today = date_utils.start_of_day(date_utils.utc_now())
    date1 = today - datetime.timedelta(days=28)
    date2 = today - datetime.timedelta(days=21)
    students = UserProfile.objects.filter(created_on__gte=date1).filter(created_on__lt=date2)
    students = students.exclude(_user_settings__contains='"user_type":"parent"')
    analytics.init(settings.SEGMENTIO_KEY)

    for student in students:
        print student
        # if not a coaching student
        if not CoachStudent.objects.filter(student=student).exists():
            parent_email = student.user_settings.get('parent_email')
            print parent_email
            if parent_email:
                # test type is not needed yet.  we will aggregate accross tests
                try:
                    user_ability = UserAbility.objects.get(user=student)
                    user_ability.update()
                    student_data = user_ability.average_questions_practiced()
                    print student_data
                except:
                    student_data = 0

                analytics.identify(parent_email, dict(student_average_questions_practiced=student_data))
                analytics.track(parent_email, 'Free Student Parent Three Week Checkin')


@shared_task
def send_program_update_email(previous_status, program_event):
    """ Alert coaches when program event changes
    """
    subject = '[COACHING] Program Updated - %s' % (program_event.coach_student.student.profile.display_name)

    body = """
    Coach Program Event: %s <br/><br/>
    Student Name: %s <br/>
    Coach Name: %s <br/><br/>
    Kickoff Date: %s <br/>
    Projected End Date: %s <br/><br/>

    Old Student State: %s <br/>
    New Student State: %s <br/><br/>
    Notes: %s <br/><br/>
    Effective Date: %s
    """ % (dict(CoachStudentProgramHistory.STATE_OPTIONS)[program_event.program_state],
           program_event.coach_student.student.profile.display_name, program_event.coach_student.coach.profile.display_name,
           program_event.coach_student.program_start_date.strftime("%Y-%m-%d"), program_event.coach_student.program_end_date.strftime("%Y-%m-%d"),
           dict(CoachStudent.STATUS_CHOICES)[previous_status], dict(CoachStudent.STATUS_CHOICES)[program_event.coach_student.status],
           program_event.program_notes,
           program_event.date_stamp.strftime("%Y-%m-%dT%H:%M:%SZ"))

    recipient_list = ['success@testive.com']
    messaging_utils.send_plain_text_email(subject, body, recipient_list)


@shared_task
def send_red_flag_email(coach_student, notes):
    """ Alert fired when a coach red flags a student account
    """
    subject = '[COACHING] Red Flag - %s' % (coach_student.student.profile.display_name)

    body = '%s says %s has a problem: <br/><br/> %s' % (coach_student.coach.profile.display_name,
                                                coach_student.student.profile.display_name,
                                                notes.replace('\n', '<br/>'))

    recipient_list = ['happy@testive.com']
    messaging_utils.send_plain_text_email(subject, body, recipient_list)


@shared_task
def invite_student_email(student_relationship):
    """Sends student an email inviting them to try Testive.
    """
    supporter = student_relationship.supporter
    student = student_relationship.user

    subject = 'I want you to join Testive!'
    data = dict(student=student, supporter=supporter,
                login_link=student.profile.login_url)
    recipients = [student.email, ]
    messaging_utils.send_email('fox_invite_student_email.html', data, recipients, subject)


@shared_task
def send_registration_to_webhook(form_data):
    requests.post(settings.ZAPIER_ADD_STUDENT_HOOK, data=form_data)


@shared_task
def notify_student_info(email_data):
    chargebee.configure(settings.CHARGEBEE_KEY, settings.CHARGEBEE_SITE)
    subscription = chargebee.Subscription.retrieve(email_data['subscription_id'])

    recipients = [email for email in settings.TUTORING_SCHEDULING_EMAIL_TO['testive']]
    subject = '[CoachingPurchase] from %s %s' % (
        subscription.customer.first_name, subscription.customer.last_name)
    data = dict(data=subscription, email_data=email_data)
    messaging_utils.send_email('purchase_email_student_info.html', data, recipients, subject)


@shared_task
def notify_student_info_for_advisor(email_data):
    chargebee.configure(settings.CHARGEBEE_KEY, settings.CHARGEBEE_SITE)
    subscription = chargebee.Subscription.retrieve(email_data['subscription_id'])

    recipients = [email for email in settings.TUTORING_SCHEDULING_EMAIL_TO['testive']]
    subject = '[AdvisorPurchase] from %s %s' % (
        subscription.customer.first_name, subscription.customer.last_name)
    data = dict(data=subscription, email_data=email_data)
    messaging_utils.send_email('purchase_email_student_info_advisor.html', data, recipients, subject)


@shared_task
def update_coaching_student_status(student):
    try:
        external = UserExternalService.objects.get(user=student)
    except UserExternalService.DoesNotExist:
        # email us
        return

    chargebee_id = external.chargebee_subscription_id
    if not chargebee_id:
        # email us
        return

    coach_students = CoachStudent.objects.filter(student=student).order_by('-id')
    if coach_students.exists():
        coach = coach_students[0].coach
        tests = coach_students[0].tests
    else:
        coach = dict(email='None', profile=dict(display_name='None'), )
        tests = 'None'

    chargebee.configure(settings.CHARGEBEE_KEY, settings.CHARGEBEE_SITE)
    subscription = chargebee.Subscription.retrieve(chargebee_id)

    recipients = [email for email in settings.TUTORING_SCHEDULING_EMAIL_TO['testive']]
    subject = '[CoachingPurchase] from %s %s' % (
        subscription.customer.first_name, subscription.customer.last_name)
    data = dict(data=subscription, student=student, coach=coach, tests=tests)
    messaging_utils.send_email('purchase_email.html', data, recipients, subject)


@shared_task
def send_referral_notification(referral, referred_from):
    """ Alert Molly/Nak when a referral is submitted via the parent dashboard.
    """
    subject = '[REFERRAL] New Referral from Parent Dashboard'

    body = """
    New Referral from Parent Dashboard: <br/>
    Referral Name: %s <br/>
    Referral Email: %s <br/>
    Referral Phone: %s <br/><br/>
    Referred By: %s <br/>
    """ % (referral.first_name + ' ' + referral.last_name, referral.email,
           referral.profile.phone_number,
           referred_from.first_name + ' ' + referred_from.last_name)

    recipient_list = settings.SEND_REFERRAL_NOTIFICATION_TO
    messaging_utils.send_plain_text_email(subject, body, recipient_list)


class NextActionsBase(object):
    """Base class for auto-population of coaching NextActions to the database.

       The pattern here is to create a subclass for each type of NextAction we
       wish to generate.

       The only public method of this and all subclasses should be .create(),
       which should not be overwritten.
    """
    def _create_next_action(self, coach, student, action_type, **kwargs):
        """Creates a NextAction object for the given coach, student, and
           action_type.

           Note: in order to avoid repeated actions we will never create
           the same type of action for the given coach/student pair within
           24 hours of each other.
        """
        one_day_ago = date_utils.utc_now() - datetime.timedelta(days=1)
        recent_actions = NextActions.objects.filter(
            coach=coach, student=student, action_type=action_type,
            created_on__gt=one_day_ago)
        if recent_actions.exists():
            return

        next_action = NextActions.objects.create(
            coach=coach, student=student, action_type=action_type)
        next_action.action_details = kwargs
        next_action.save()

    def _create(self, coach, students):
        """Helper method to provide an interface for each subclass to create
           unique next actions. Should be overwritten by children.
        """
        return

    def create(self, coach, students):
        """Main public method. This is called by the NextActions task in order
           to create next actions of the expected type.
        """
        for student in students:
            self._create(coach, student)

    def _get_study_day_on_date(self, student, date):
        """Returns study_day on the given date, if one exists.
        """
        try:
            study_day = StudyDay.objects.get(user_id=student.id,
                                             date=date)
            return study_day
        except StudyDay.DoesNotExist:
            return None

    def _user_studied_today_or_yesterday(self, student):
        """Did the given user study today or yesterday?
        """
        study_today = self._get_study_day_on_date(student, date_utils.utc_now())

        if not study_today or not study_today.user_has_studied:
            yesterday = date_utils.utc_now() - datetime.timedelta(days=1)
            study_yesterday = self._get_study_day_on_date(student, yesterday)

            if not study_yesterday or not study_yesterday.user_has_studied:
                return False

        return True


class MissedWorkActions(NextActionsBase):
    """
    """
    def _create(self, coach, student):
        now = date_utils.utc_now()
        a_week_ago = now - datetime.timedelta(days=6)

        study_days_last_week = StudyDay.objects.filter(
            user=student, date__range=(a_week_ago, now))

        two_weeks_ago = a_week_ago - datetime.timedelta(days=6)
        study_days_two_weeks_ago = StudyDay.objects.filter(
            user=student, date__range=(two_weeks_ago, a_week_ago))

        if study_days_last_week.count() == 7:
            percent_completed_last_week = self._percent_completed(study_days_last_week)
        else:
            percent_completed_last_week = 1

        if percent_completed_last_week < 0.5:

            if study_days_two_weeks_ago.count() == 7:
                percent_completed_two_weeks = self._percent_completed(study_days_two_weeks_ago)
            else:
                percent_completed_two_weeks = 1

            if percent_completed_two_weeks < 0.5:
                action_type = NextActions.MISSED_HALF_WORK_RED_FLAG
            else:
                action_type = NextActions.MISSED_HALF_WORK

            self._create_next_action(
                coach,
                student,
                action_type,
            )

    def _percent_completed(self, study_days):
        commitment = 0
        completed = 0

        for study_day in study_days:
            commitment += study_day.question_commitment
            completed += study_day.questions_answered

        if commitment == 0:
            percent_completed = 1
        else:
            percent_completed = float(completed) / commitment

        return percent_completed


class NoMeetingsActions(NextActionsBase):
    """
    """
    def _create(self, coach, student):
        now = date_utils.utc_now()

        all_meetings = Meeting.objects.filter(user=student, staff=coach)

        # first check if there are no meetings
        if all_meetings.count() == 0:
            return  # Note -- We should have a special event for this.
        elif all_meetings.count() == 1:  # Just the one meeting, probably a kickoff
            if all_meetings[0].date_time > now:  # If the kickoff hasn't happened yet
                return

        ten_days_ago = now - datetime.timedelta(days=9)

        meetings_in_last_ten = Meeting.objects.filter(
            user=student, staff=coach, date_time__range=(ten_days_ago, now))

        if meetings_in_last_ten.count() == 0:

            thirteen_days_ago = now - datetime.timedelta(days=12)

            meetings_in_last_thirteen = Meeting.objects.filter(
                user=student, staff=coach, date_time__range=(thirteen_days_ago, now))

            if meetings_in_last_thirteen.count() == 0:
                action_type = NextActions.NO_MEETING_13_DAY
            else:
                action_type = NextActions.NO_MEETING_10_DAY

            self._create_next_action(
                coach,
                student,
                action_type,
            )


class MissedTwoDaysInRowAction(NextActionsBase):

    def _create(self, coach, student):
        missed, session_date = self._get_missed_session(student)
        if missed:
            self._create_next_action(coach, student, NextActions.MISSED_TWO_DAYS_IN_ROW, date=session_date)

    def _get_missed_session(self, student):

        now = date_utils.utc_now()

        study_days = StudyDay.objects.filter(user=student, day_type=StudyDay.STUDY, date__lt=now)
        study_days = study_days.order_by('-date')

        if len(study_days) > 1:
            last_study_session = study_days[0]
            two_study_sessions_ago = study_days[1]
        else:
            return (False, '')

        if last_study_session.missed:
            if two_study_sessions_ago.missed:
                return (True, two_study_sessions_ago.date)

        return (False, two_study_sessions_ago.date)


class UploadedPracticeTestAction(NextActionsBase):

    def _create(self, coach, student):
        bluebook_sections_completed, session_date = self._get_practice_test(student)
        if bluebook_sections_completed >= 1:
            self._create_next_action(coach, student, NextActions.UPLOADED_PRACTICE_TEST, date=session_date)

    def _get_practice_test(self, student):
        now = date_utils.utc_now()
        practice_test_days = StudyDay.objects.filter(user=student, date=now)

        if practice_test_days.exists():
            practice_test_day = practice_test_days[0]
            if practice_test_day.bluebook_sections_completed > 0:
                return (practice_test_day.bluebook_sections_completed, now)
        return(0, now)


class UpdateCoachNextActions(object):
    """Auto-populates the database with a series of next actions for coaches to
       take with students.

       Each type of action we are creating will have their own class, which
       handles the generation of the actions themselves.
    """
    def _get_students(self, coach):
        student_relationships = CoachStudent.objects.filter(
            coach=coach, status=CoachStudent.ACTIVE)
        students = []
        for relationship in student_relationships:
            students.append(relationship.student)
        return students

    def run(self, override=False):
        coaches = CoachOrganization.objects.all()

        missed_work_generator = MissedWorkActions()
        no_meeting_generator = NoMeetingsActions()
        missed_two_days_generator = MissedTwoDaysInRowAction()
        uploaded_practice_test_generator = UploadedPracticeTestAction()

        for coach in coaches:
            students = self._get_students(coach.coach)

            if date_utils.utc_now().weekday() == 0 or override is True:  # Only run on Mondays
                missed_work_generator.create(coach.coach, students)

            no_meeting_generator.create(coach.coach, students)
            missed_two_days_generator.create(coach.coach, students)
            uploaded_practice_test_generator.create(coach.coach, students)


@periodic_task(run_every=crontab(minute=0, hour=6))
def update_coach_next_actions():
    UpdateCoachNextActions().run()


if __name__ == '__main__':
    update_coach_next_actions()


class CoachReportTasks(object):
    """Background tasks run in background to continuously update coach reports
    """
    def _get_coach_students(self, coach):
        """Caches and returns the list of studnents for this coach
        """
        key = '__coach_students_%s' % coach.id
        if not hasattr(self, key):
            coach_student_list = CoachStudent.objects.filter(coach=coach, status=CoachStudent.ACTIVE)
            setattr(self, key, list(coach_student_list))
        return getattr(self, key)

    def _get_students_for_test(self, coach, test):
        """Return the list of students for this coach who are studing for the specified test (SAT or ACT)
        """
        test_students = []
        coach_student_list = self._get_coach_students(coach)
        for coach_student in coach_student_list:
            if test in coach_student.tests:
                test_students.append(coach_student.student)
        return test_students

    def _get_students(self, coach):
        """Return all students for this coach
        """
        coach_student_list = self._get_coach_students(coach)
        return [x.student for x in coach_student_list]

    def _get_parents(self, students):
        """Return all parents for the list of students
        """
        parent_list = UserSupporter.objects.filter(user__in=students, supporter_type=UserSupporter.PARENT)
        return [x.supporter for x in parent_list]

    def _divide(self, numerator, denominator):
        if denominator > 0:
            return float(numerator) / float(denominator)
        return 0

    def get_last_contact(self, students, coach):
        """Calculates the number of days since the given user has recieved an
           email / text / meeting from a non-parent supporter or an
           email / text has been exchanged between a supporter and
           the given user's parents.
        """
        total = 0
        days_since_sent = 0
        for student in students:
            student_supporters = UserSupporter.objects.filter(user=student, supporter_type=UserSupporter.PARENT)
            messages = UserMessage.objects.filter(
                Q(sent_by=coach, sent_to=student, message_type__in=(UserMessage.EMAIL, UserMessage.TEXT, UserMessage.NOTE)) |
                Q(sent_by=coach, sent_to__in=student_supporters, message_type=UserMessage.EMAIL)
            )

            if messages.count() > 0:
                latest_message = messages.latest('created_on')
                days_since_sent += (date_utils.utc_now() - latest_message.created_on).days
                total += 1

        return days_since_sent, total

    def get_parent_communication_count(self, students, coach):
        """# of families contacted in last 10 days/#students
        """
        communications_count = 0
        start_date = date_utils.utc_now() - datetime.timedelta(days=10)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        for student in students:
            parent_list = self._get_parents([student])
            messages = UserMessage.objects.filter(sent_by=coach,
                                                  sent_to__in=parent_list,
                                                  message_type=UserMessage.EMAIL,
                                                  created_on__gt=start_date,)
            if messages.count() > 0:
                communications_count += 1

        return communications_count

    def get_last_student_meeting(self, students, coach):
        """Calculates the number of days since the last meeting between the
           user and self.user (the coach).
        """
        meetings = Meeting.objects.values('user', 'date_time').filter(
            staff=coach,
            user__in=students,
            date_time__lte=date_utils.utc_now()
        )
        meetings = meetings.values('user').annotate(most_recent=Max('date_time'))

        messages = UserMessage.objects.values('sent_to', 'created_on').filter(
            sent_by=coach,
            sent_to__in=students,
            message_type=UserMessage.NOTE,
            created_on__lte=date_utils.utc_now()
        )
        messages = messages.values('sent_to').annotate(most_recent=Max('created_on'))

        # now we need to determine the min for each user
        count = 0
        days_since_meeting = 0

        for student in students:
            most_recent = None
            mtg = meetings.filter(user=student)
            msg = messages.filter(sent_to=student)

            if mtg.exists() and msg.exists():
                most_recent = max(mtg[0]['most_recent'], msg[0]['most_recent'])
            elif mtg.exists():
                most_recent = mtg[0]['most_recent']
            elif msg.exists():
                most_recent = msg[0]['most_recent']

            if most_recent:
                days_since_meeting += (date_utils.utc_now() - most_recent).days
                count += 1

        return days_since_meeting, count

    def get_survey_responses(self, page=1, responses=[]):
        """Returns a list of all NPS survey responses from Delighted.
        """
        responses = []
        if settings.IS_PRODUCTION:
            client = Delighted(settings.DELIGHTED_API_KEY)
            params = {'page': page, 'per_page': 100, 'order': 'desc', 'expand[]': 'person'}
            response_batch = client.survey_response.get(**params)
            responses += response_batch
            if len(response_batch) == 100:
                return self.get_survey_responses((page + 1), responses)
            return responses
        return responses

    def _get_nps(self, email, all_responses):
        """Returns the most recent NPS survey result for the given email address.
        """
        for response in all_responses:
            if response['person']['email'] == email:
                return response['score']
        return '-'

    def get_avg_nps(self, people, all_responses):
        """get student nps from delighted and or our database for each student in students list
        """
        scores = []
        for person in people:
            get_score = self._get_nps(person.email, all_responses)
            if get_score != '-':
                scores.append(get_score)
        total = 0
        for each in scores:
            if each > 8:
                total += 100
            elif each < 7:
                total -= 100
        return total, len(scores)

    def get_student_grades(self, students):
        total_grade = 0
        for student in students:
            report_card = ReportCard(student)
            student_grade = report_card.serialized
            total_grade += student_grade['overall_grade']['earned_points']
        return total_grade

    def get_score_improvement(self, coach, test):
        improvement = 0

        students = self._get_students_for_test(coach, test)
        abilities = UserAbility.objects.filter(user__in=students)
        for user_ability in abilities:
            improvement += user_ability.score_improvement(test)
        return improvement, len(students)

    def run(self):

        coach_loads = CoachLoad.objects.all()

        total_coach_count = len(coach_loads)

        gt_students_met = 0
        gt_total_days_since_mtg = 0
        gt_parent_contact_count = 0
        gt_student_count = 0
        gt_max_load = 0
        gt_days_since_last_contact = 0
        gt_students_contacted = 0
        gt_student_grades = 0
        gt_score_improvement_act = 0
        gt_score_improvement_sat = 0
        gt_score_count_act = 0
        gt_score_count_sat = 0
        gt_nps_student = 0
        gt_nps_count_student = 0
        gt_nps_parent = 0
        gt_nps_count_parent = 0

        coach_report_list = []
        for coach_load in coach_loads:

            gt_max_load += coach_load.max_load
            coach = coach_load.coach

            students = self._get_students(coach)
            student_count = len(students)
            gt_student_count += student_count

            if student_count > 0:
                # metric act score improvement
                score_improvement_act, score_count_act = self.get_score_improvement(coach, 'ACT')
                gt_score_improvement_act += score_improvement_act
                gt_score_count_act += score_count_act

                # metric sat score improvement
                score_improvement_sat, score_count_sat = self.get_score_improvement(coach, 'SAT')
                gt_score_improvement_sat += score_improvement_sat
                gt_score_count_sat += score_count_sat

                # metric = parent_contact
                parent_contact_count = self.get_parent_communication_count(students, coach)
                gt_parent_contact_count += parent_contact_count

                # metric = avg_last_contact
                days_since_last_contact, students_contacted = self.get_last_contact(students, coach)
                gt_days_since_last_contact += days_since_last_contact
                gt_students_contacted += students_contacted

                # metric = avg_meets
                days_since_last_mtg, students_met = self.get_last_student_meeting(students, coach)
                gt_total_days_since_mtg += days_since_last_mtg
                gt_students_met += students_met

                # metric = effort grade
                student_grades = self.get_student_grades(students)
                gt_student_grades += student_grades

                # nps
                survey_responses = self.get_survey_responses()

                # metric = student nps
                nps_student, nps_count_student = self.get_avg_nps(students, survey_responses)
                gt_nps_student += nps_student
                gt_nps_count_student += nps_count_student

                # metric = parent nps
                parents = self._get_parents(students)
                nps_parent, nps_count_parent = self.get_avg_nps(parents, survey_responses)
                gt_nps_parent += nps_parent
                gt_nps_count_parent += nps_count_parent

            coach_report_list.append(CoachReport(
                coach=coach,
                current_load=student_count,
                max_load=coach_load.max_load,
                avg_effort_grade=self._divide(student_grades, student_count),
                score_improvement_sat=self._divide(score_improvement_sat, score_count_sat),
                score_improvement_act=self._divide(score_improvement_act, score_count_act),
                avg_last_contact=self._divide(days_since_last_contact, students_contacted),
                parent_contact=self._divide(parent_contact_count, student_count),
                avg_meets=self._divide(days_since_last_mtg, students_met),
                avg_student_nps=self._divide(nps_student, nps_count_student),
                avg_parent_nps=self._divide(nps_parent, nps_count_parent)))

        # add the grand total row
        coach_report_list.append(CoachReport(
            coach=None,
            current_load=self._divide(gt_student_count, total_coach_count),
            max_load=self._divide(gt_max_load, total_coach_count),
            avg_effort_grade=self._divide(gt_student_grades, gt_student_count),
            score_improvement_sat=self._divide(gt_score_improvement_sat, gt_score_count_sat),
            score_improvement_act=self._divide(gt_score_improvement_act, gt_score_count_act),
            avg_last_contact=self._divide(gt_days_since_last_contact, gt_students_contacted),
            parent_contact=self._divide(gt_parent_contact_count, gt_student_count),
            avg_meets=self._divide(gt_total_days_since_mtg, gt_students_met),
            avg_student_nps=self._divide(gt_nps_student, gt_nps_count_student),
            avg_parent_nps=self._divide(gt_nps_parent, gt_nps_count_parent)))

        CoachReport.objects.bulk_create(coach_report_list)


# Run this stuff all the time
@periodic_task(run_every=datetime.timedelta(hours=3))
def calculate_coach_report():
    CoachReportTasks().run()


class PopulateStudentRecord(object):
    """Task to periodically create StudentRecord objects for every
       currently active coaching student.
    """
    def _calc_effort_grade(self, user):
        """Returns a serialized set of grades for a user's performance.
        """
        report_card = ReportCard(user)
        return report_card.serialized

    def _calc_score_improvement(self, user, tests):
        user_ability = UserAbility.objects.get(user=user)
        output = {}
        for test in tests:
            output[test] = user_ability.score_improvement(test)
        return output

    def _calc_days_since_last_contact(self, user, coach):
        """Calculates the number of days since the given user has recieved an
           email / text / meeting from a non-parent supporter or an
           email / text has been exchanged between a supporter and
           the given user's parents.
        """
        from messaging.models import UserMessage

        coaches = [coach]

        supervisor = self._get_coach_supervisor(coach)
        if supervisor:
            coaches.append(supervisor)

        parents = UserSupporter.objects.filter(user=user, supporter_type=UserSupporter.PARENT)
        parents = [relationship.supporter for relationship in parents]

        messages = UserMessage.objects.filter(Q(sent_by__in=coaches, sent_to=user,
                                                message_type__in=(UserMessage.EMAIL,
                                                                  UserMessage.TEXT,
                                                                  UserMessage.NOTE)) |
                                              Q(sent_by__in=coaches, sent_to__in=parents,
                                                message_type=UserMessage.EMAIL))
        try:
            latest_message = messages.latest('created_on')
            days_since_sent = (date_utils.utc_now() - latest_message.created_on).days
            return days_since_sent
        except UserMessage.DoesNotExist:
            return -1

    def _calc_days_since_last_meeting(self, user, coach):
        """Calculates the number of days since the last meeting between the
           user and self.user (the coach).
        """
        from meetings.models import Meeting
        from messaging.models import UserMessage

        meetings = Meeting.objects.filter(
            staff=coach, user=user, date_time__lte=date_utils.utc_now())

        try:
            latest_meeting = meetings.latest('date_time')
            days_since_meeting = (date_utils.utc_now() - latest_meeting.date_time).days
        except Meeting.DoesNotExist:
            days_since_meeting = -1

        meeting_notes = UserMessage.objects.filter(
            sent_by=coach, sent_to=user, message_type=UserMessage.NOTE)

        try:
            latest_note = meeting_notes.latest('created_on')
            days_since_note = (date_utils.utc_now() - latest_note.created_on).days

            if days_since_note < days_since_meeting or days_since_meeting == -1:
                days_since_meeting = days_since_note
        except UserMessage.DoesNotExist:
            pass

        return days_since_meeting

    def _get_coach_supervisor(self, coach):
        try:
            coach_org = CoachOrganization.objects.get(coach=coach)
        except CoachOrganization.DoesNotExist:
            return None
        return coach_org.supervisor

    def run(self):
        active_coach_students = CoachStudent.objects.filter(status=CoachStudent.ACTIVE)
        for coach_student in active_coach_students:
            try:
                student = coach_student.student
                coach = coach_student.coach

                report = StudentRecord(student=student)
                report.effort_grade = self._calc_effort_grade(student)
                report.score_improvement = self._calc_score_improvement(student, coach_student.tests)
                report.days_since_last_contact = self._calc_days_since_last_contact(student, coach)
                report.days_since_last_meeting = self._calc_days_since_last_meeting(student, coach)
                report.save()
            except Exception:
                continue


@periodic_task(run_every=datetime.timedelta(hours=2))
def calculate_student_record():
    PopulateStudentRecord().run()


class AsyncProgramStudentGroups(object):

    def run(self):
        """Loop over all students in the studentgroup table and
           group them according to the section score compared to their
           composite score
        """
        # (reading, math, writing)
        group_definition = {
            ('H', 'H', 'H'): StudentGroup.GROUP_A,
            ('L', 'H', 'L'): StudentGroup.GROUP_B,
            ('H', 'L', 'L'): StudentGroup.GROUP_C,
            ('L', 'L', 'H'): StudentGroup.GROUP_D,
            ('H', 'H', 'L'): StudentGroup.GROUP_E,
            ('H', 'L', 'H'): StudentGroup.GROUP_F,
            ('L', 'H', 'H'): StudentGroup.GROUP_G
        }

        student_groups = StudentGroup.objects.all()
        for student_group in student_groups:
            try:
                current_ability = UserAbility.objects.get(user=student_group.student)
                section_scores = current_ability.get_section_scores('NEW_SAT')
                if section_scores is None:
                    student_group.group = StudentGroup.BAD_DATA
                    student_group.save()
                    continue

                average_score = sum(section_scores.values()) / len(section_scores.values())
                reading = 'L' if section_scores['reading'] < average_score else 'H'
                writing = 'L' if section_scores['writing_and_language'] < average_score else 'H'
                math = float(section_scores['math_calculator'] + section_scores['math_no_calculator']) / 2
                math = 'L' if math < average_score else 'H'

                score_comparison = (reading, math, writing)
                student_group.group = group_definition[score_comparison]
                student_group.save()
            except:
                student_group.group = StudentGroup.BAD_DATA
                student_group.save()


@periodic_task(run_every=crontab(day_of_week='thursday', hour=23, minute=0))
def group_async_students():
    AsyncProgramStudentGroups().run()
