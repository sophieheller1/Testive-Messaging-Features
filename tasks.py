"""Tasks for the coaching app.
"""
import datetime
import analytics
import requests

from delighted import Delighted
from django.db.models import Q, Max
from django.conf import settings

from hbs import date_utils, messaging_utils

from accounts.models import UserExternalService, UserProfile
from coaching.models import (NextActions, UserSupporter, CoachStudent,
                             CoachOrganization, CoachLoad, CoachReport,
                             StudentRecord, CoachStudentProgramHistory, StudentGroup)


""" Used predefined class NextActionsBase to add two classes that inherit from it: 
  MissedTwoDaysinRowAction and UploadedPracticeTestAction"""

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



""" Created messaging functionality in response to students who have missed two consecutive study days
on their predfined Testive study calendar. This class will detect any instance of 2 missed days, regardless of if the 
missed days are consecutive week days and will create an event in the Coach inbox. This event will prompt the coach to 
send a text to the student through Testive's predefined Twilio messaging services."""
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

""" Created messaging functionality in response to students who have uploaded a practice test to their accounts. This class will detect one or more newly added practice test sections
and will create an event in the Coach inbox. This event will prompt the coach to 
view the uploaded practice test directly in their inbox panel. """
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

"""Predfined handler that I used to generate actions for my two new events """
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
